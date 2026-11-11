#!/usr/bin/env python3
"""Write worker_instances.qnt: one module per ack mode, design and fault
class. Edit the tables here and run it again."""
from pathlib import Path

# design name -> (base, overrides)
DESIGNS = {
    'main': ('MAIN', ''),
    'pr': ('PR', ''),
    # 1. flush() keeps a job the child already has
    'keepwritten': ('PR', 'flush: DropUnwritten'),
    # 2. renewal of unacked messages
    'renewall': ('PR', 'renewal: RenewAll'),
    'renewidle': ('PR', 'renewal: RenewUnstarted'),
    'reneweta': ('PR', 'renewal: RenewScheduled'),
    # 3. a failed ack is sent again; a collected channel can still ack
    'resend': ('PR', 'failedAck: Resent'),
    'ackfull': ('PR', 'failedAck: Resent, collectedAck: StillAcks'),
    # 4. restore at reconnect what the closing channel could not
    'restoreall': ('PR', 'failedAck: Resent, collectedAck: StillAcks, '
                         'reconnectRestore: ReconnectRestoresAll'),
    'restoreidle': ('PR', 'failedAck: Resent, collectedAck: StillAcks, '
                          'reconnectRestore: ReconnectRestoresIdle'),
    # 6. and a closing channel leaves running tasks alone
    'best': ('PR', 'flush: DropUnwritten, renewal: RenewScheduled, '
                   'failedAck: Resent, collectedAck: StillAcks, '
                   'closeRestore: CloseRestoresIdle, '
                   'reconnectRestore: ReconnectRestoresIdle, '
                   'restoreBy: ByPop'),
    # a channel restores only a record its own pop wrote (both with the
    # flush variant, so that the restore is the only source of a third run)
    # the pop token on its own: every fault class of the PR
    'token': ('PR', 'restoreBy: ByPop'),
    'tagrestore': ('PR', 'flush: DropUnwritten'),
    'ownrecord': ('PR', 'flush: DropUnwritten, restoreBy: ByPop'),
}
# fault class -> (budget, eta)
FAULTS = {
    'none': ('', False),
    'shutdown': ('shutdowns: 1, restarts: 1', False),
    'eta': ('timeouts: 1', True),
    'long': ('timeouts: 1', False),
    'kill': ('kills: 1, restarts: 1', False),
    'crash': ('crashes: 1, restarts: 2', False),
    'partition': ('partitions: 1, restarts: 1', False),
    'cut': ('cuts: 1, restarts: 1', False),
    'cutlong': ('cuts: 1, timeouts: 1, restarts: 1', False),
    'reset': ('resets: 1, restarts: 1', False),
    'hang': ('hangs: 1', False),
    # an ETA outlasts the timeout, then its worker shuts down
    'etashutdown': ('timeouts: 1, shutdowns: 1, restarts: 1', True),
}
EVERY = [f for f in FAULTS if f != 'etashutdown']
# design -> ack modes -> fault classes
MATRIX = {
    'main': {'late': ['none', 'shutdown', 'eta', 'kill', 'partition'],
             'early': ['none', 'shutdown', 'partition']},
    'token': {'late': EVERY, 'early': ['shutdown', 'eta', 'cut', 'reset']},
    'tagrestore': {'late': ['etashutdown']},
    'ownrecord': {'late': ['etashutdown']},
    'pr': {'late': EVERY,
           'early': ['none', 'shutdown', 'eta', 'long', 'kill', 'cut',
                     'reset', 'hang']},
    'keepwritten': {'late': ['shutdown', 'reset', 'cut'],
                    'early': ['shutdown', 'cut']},
    'renewall': {'late': ['eta', 'long', 'hang', 'kill', 'cutlong'],
                 'early': ['eta', 'long']},
    'renewidle': {'late': ['eta', 'long', 'hang', 'kill'], 'early': ['long']},
    'reneweta': {'late': ['eta', 'long', 'hang', 'kill']},
    'resend': {'late': ['cut', 'crash', 'partition'], 'early': ['cut']},
    'ackfull': {'late': ['cut', 'crash', 'reset', 'cutlong'],
                'early': ['cut']},
    'restoreall': {'late': ['cut', 'crash']},
    'restoreidle': {'late': ['cut', 'crash', 'kill']},
    'best': {'late': EVERY, 'early': ['none', 'shutdown', 'eta', 'cut']},
}

out = ['''// -*- mode: Bluespec; -*-
// Instances of worker.qnt, written by gen_worker_instances.py:
// <design>_<ack mode>_<fault class>.
//
//   quint run --main=<instance> worker_instances.qnt --invariant=<property>
//
// ---- instances: a design, an ack mode and a fault class -------------------
''']
for design, modes in MATRIX.items():
    base, over = DESIGNS[design]
    d = f'{{ ...{base}, {over} }}' if over else base
    for ack, classes in modes.items():
        for fault in classes:
            budget, eta = FAULTS[fault]
            fields = [f'...config({ack.capitalize()}, {d})']
            if eta:
                fields.append('eta: Set(M(1)), outlasts: OnlyEta')
            if budget:
                fields.append(f'max: {{ ...NONE, {budget} }}')
            body = ',\n    '.join(fields)
            out.append(f'''module {design}_{ack}_{fault} {{
  import workertypes.* from "./worker"
  import worker(CFG = {{
    {body},
  }}).* from "./worker"
}}
''')
Path(__file__).with_name('worker_instances.qnt').write_text(''.join(out))
print(sum(len(c) for m in MATRIX.values() for c in m.values()), 'instances')
