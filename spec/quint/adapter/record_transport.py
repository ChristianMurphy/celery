"""Code to spec for redis_transport.qnt: record what two kombu consumers
and a publisher send to a real Redis, and check that the spec allows it.

Two consumers with prefetch 1 and two messages on one queue, as in the
spec. A random schedule publishes, lets either consumer read, and acks a
held message. Redis MONITOR records every command with the connection that
sent it (each kombu connection names its sockets with CLIENT SETNAME) and
the commands the pop script runs. The commands become a `quint test` run:
a run passes when every action is enabled in the order the code took it.

    # record, with the kombu tree to observe on PYTHONPATH (Redis on 6379)
    PYTHONPATH=<kombu> python -m adapter.record_transport --design pr --n 30
    # check the recorded histories against the spec
    python -m adapter.record_transport --design pr --check

--design main is BRPOP then record (445027e), pr is the pop script
(kombu PR 2831).
"""
from __future__ import annotations

import argparse
import json
import random
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
URL = 'redis://127.0.0.1:6379/12'
DESIGN = {'main': 'BrpopThenRecord', 'pr': 'PopScript'}


def record_one(rng: random.Random) -> dict:
    import redis
    from kombu import Connection, Consumer, Producer, Queue

    raw = redis.Redis(port=6379, db=12)
    raw.delete('unacked', 'unacked_index')
    queue = Queue(f'rec-{uuid.uuid4().hex}')
    log: list[dict] = []
    ready = threading.Event()

    def monitor() -> None:
        with redis.Redis(port=6379, db=12).monitor() as m:
            ready.set()
            for e in m.listen():
                if e['command'] == 'PING stop-monitor':
                    return
                log.append({'port': e['client_port'], 'type': e['client_type'],
                            'cmd': e['command']})

    watcher = threading.Thread(target=monitor, daemon=True)
    watcher.start()
    ready.wait(5)
    time.sleep(0.1)
    conns = {name: Connection(URL, transport_options={'client_name': name})
             for name in ('pub', 'c1', 'c2')}
    Producer(conns['pub']).maybe_declare(queue)
    held: dict[str, list] = {'c1': [], 'c2': []}
    consumers = []
    for name in ('c1', 'c2'):
        consumer = Consumer(conns[name], [queue], prefetch_count=1,
                            on_message=held[name].append)
        consumer.consume()
        consumers.append(consumer)

    def drain(name: str) -> None:
        try:
            conns[name].drain_events(timeout=0.05)
        except (socket.timeout, TimeoutError):
            pass

    published = acked = 0
    deadline = time.monotonic() + 20
    while acked < 2 and time.monotonic() < deadline:
        choices = ['c1', 'c2', 'c1', 'c2']
        if published < 2:
            choices.append('publish')
        choices += [f'ack {n}' for n in held if held[n]]
        step = rng.choice(choices)
        if step == 'publish':
            published += 1
            Producer(conns['pub']).publish(
                {'n': published}, exchange='', routing_key=queue.name)
        elif step.startswith('ack'):
            held[step[4:]].pop().ack()
            acked += 1
        else:
            drain(step)
    left = {'queue': raw.llen(queue.name), 'unacked': raw.hlen('unacked')}
    raw.execute_command('PING', 'stop-monitor')
    watcher.join(5)
    for conn in conns.values():
        conn.release()
    raw.delete(queue.name)
    return {'queue': queue.name, 'log': log, 'acked': acked, 'left': left}


def to_run(i: int, design: str, history: dict) -> str:
    """The recorded commands as actions of the spec."""
    queue = history['queue']
    names: dict[str, str] = {}      # port -> connection name
    tags: dict[str, int] = {}       # delivery tag -> message number
    state = {'c1': 'idle', 'c2': 'idle'}
    steps: list[str] = []
    pops: list[str] = []            # consumers whose script is running
    log = history['log']

    def consumer(name: str) -> str:
        return f'C({name[1]})'

    for n, e in enumerate(log):
        words = e['cmd'].split(' ')
        op = words[0].upper()
        if e['type'] == 'lua':
            # a command of the pop script that the last EVALSHA started
            if op == 'ZADD' and pops:
                c = pops.pop()
                tag = next(w for w in words if w in tags)
                steps += [f'serverPop({consumer(c)}, M({tags[tag]}), false)',
                          f'receive({consumer(c)})']
                state[c] = 'holding'
            continue
        if pops:
            # the script ran and recorded nothing: every queue was empty
            c = pops.pop()
            if state[c] == 'popping':
                steps.append(f'popEmpty({consumer(c)})')
            state[c] = 'waiting'
        if op == 'CLIENT' and words[1].upper() == 'SETNAME':
            names[e['port']] = words[2]
            continue
        who = names.get(e['port'])
        if who == 'pub' and op == 'LPUSH' and words[1] == queue:
            body = e['cmd'].split(' ', 2)[2]
            tag = json.loads(json.loads(f'"{body}"') if body.startswith('\\')
                             else body)['properties']['delivery_tag']
            tags[tag] = len(tags) + 1
            steps.append(f'publish(M({tags[tag]}))')
        elif who in state:
            c = who
            if op == 'BRPOP' and state[c] == 'idle':
                steps.append(f'sendPop({consumer(c)})')
                state[c] = 'popping'
            elif op == 'EVALSHA' and queue in e['cmd'] and any(
                    x['type'] == 'lua' and x['cmd'].upper().startswith('RPOP')
                    for x in log[n + 1:n + 2]):
                if state[c] == 'idle':
                    steps.append(f'sendPop({consumer(c)})')
                    state[c] = 'popping'
                elif state[c] == 'waiting':
                    # woken by a token, or the wait timed out; nothing to
                    # say if the queue is empty again
                    state[c] = 'woken'
                pops.append(c)
                nxt = [x for x in log[n + 1:n + 12] if x['type'] == 'lua']
                found = any(x['cmd'].upper().startswith('ZADD') for x in nxt)
                if state[c] == 'woken':
                    if found:
                        steps.append(f'any {{ wakeUp({consumer(c)}), '
                                     f'waitTimeout({consumer(c)}) }}')
                        state[c] = 'popping'
                    else:
                        state[c] = 'waiting'
            elif op == 'BLPOP' and state[c] == 'idle':
                # the last pop emptied the queue: wait without popping
                steps.append(f'waitAfterEmpty({consumer(c)})')
                state[c] = 'waiting'
            elif op == 'ZADD' and words[1] == 'unacked_index' \
                    and design == 'main':
                tag = next(w for w in words if w in tags)
                steps += [f'serverPop({consumer(c)}, M({tags[tag]}), false)',
                          f'receive({consumer(c)})']
                state[c] = 'holding'
            elif op == 'ZREM' and words[1] == 'unacked_index':
                tag = next(w for w in words if w in tags)
                steps.append(f'ack({consumer(c)}, M({tags[tag]}))')
                state[c] = 'idle'
    body = '\n'.join(f'    .then({s})' for s in steps)
    return (f'  run t{i} = init\n{body}\n'
            '    .expect(MSGS.forall(m => acked.contains(m)) '
            'and unacked == Set() and queue == Set())')


def check(design: str, path: Path) -> int:
    histories = [json.loads(line) for line in path.read_text().splitlines()]
    runs = '\n\n'.join(to_run(i, design, h) for i, h in enumerate(histories))
    mod = f'transport_{design}_histories'
    src = (f'module {mod} {{\n'
           '  import transporttypes.* from "../transport/redis_transport"\n'
           f'  import redis_transport(CFG = {{ design: {DESIGN[design]}, '
           'renewal: NoRenewal, putBack: ByScore, max: NO_FAULTS }).* '
           f'from "../transport/redis_transport"\n\n{runs}\n}}\n')
    qnt = path.parent / f'_{mod}.qnt'
    qnt.write_text(src)
    p = subprocess.run(['quint', 'test', f'--main={mod}', '--match=^t', str(qnt)],
                       cwd=HERE, capture_output=True, text=True)
    out = p.stdout + p.stderr
    passed = out.count('    ok ')
    clean = sum(h['acked'] == 2 and h['left'] == {'queue': 0, 'unacked': 0}
                for h in histories)
    print(f'{path}: {len(histories)} histories ({clean} ended with both '
          f'messages acked and Redis empty); {passed}/{len(histories)} '
          f'allowed by redis_transport.qnt ({DESIGN[design]}); quint test '
          f'exit {p.returncode}')
    if p.returncode:
        print(out[-1500:])
    return p.returncode


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--design', choices=sorted(DESIGN), required=True)
    ap.add_argument('--n', type=int, default=30)
    ap.add_argument('--seed', type=int, default=1)
    ap.add_argument('--check', action='store_true')
    args = ap.parse_args()
    path = HERE / 'histories' / f'transport_{args.design}.jsonl'
    if args.check:
        return check(args.design, path)
    rng = random.Random(args.seed)
    with path.open('w') as f:
        for _ in range(args.n):
            f.write(json.dumps(record_one(rng)) + '\n')
    print(f'recorded {args.n} histories in {path}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
