"""Brokers and result backends the harness can start, kill and restart."""
import base64
import json
import os
import signal
import subprocess
import time
import urllib.request

import redis


def _wait(predicate, timeout, what, alive=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if alive is not None and not alive():
            raise TimeoutError(f'{what} exited while starting')
        try:
            if predicate():
                return
        except Exception:  # pylint: disable=broad-except
            pass
        time.sleep(0.5)
    raise TimeoutError(f'{what} not ready after {timeout}s')


class RedisService:
    """A local redis-server process.

    ``persistence`` is one of ``none``, ``rdb``, ``aof-everysec`` or
    ``aof-always``; it decides what survives :meth:`kill`.
    """

    kind = 'redis'

    def __init__(self, name, port, data_dir, persistence='aof-everysec'):
        self.name, self.port, self.persistence = name, port, persistence
        self.data_dir = os.path.join(data_dir, name)
        os.makedirs(self.data_dir, exist_ok=True)
        self.proc = None

    def args(self):
        args = ['redis-server', '--port', str(self.port), '--dir', self.data_dir,
                '--bind', '127.0.0.1', '--save', '', '--appendonly', 'no']
        if self.persistence == 'rdb':
            args[-3:] = ['900 1 60 100', '--appendonly', 'no']
        elif self.persistence.startswith('aof'):
            args[-1] = 'yes'
            args += ['--appendfsync', self.persistence.split('-', 1)[1]]
        return args

    def start(self):
        log = open(os.path.join(self.data_dir, 'redis.log'), 'ab')
        self.proc = subprocess.Popen(self.args(), stdout=log, stderr=subprocess.STDOUT,
                                     start_new_session=True)
        _wait(lambda: self.client().ping(), 20, self.name)

    def kill(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()

    def restart(self):
        self.kill()
        self.start()

    def pause(self):
        self.proc.send_signal(signal.SIGSTOP)

    def resume(self):
        self.proc.send_signal(signal.SIGCONT)

    stop = kill

    def client(self, db=0):
        return redis.Redis(port=self.port, db=db, socket_timeout=5)

    def url(self, port=None, db=0):
        return f'redis://127.0.0.1:{port or self.port}/{db}'

    def depth(self, queue):
        """Messages still owned by the broker: ready plus unacked."""
        c = self.client()
        ready = sum(c.llen(k) for k in c.scan_iter(match=f'{queue}*') if c.type(k) == b'list')
        return ready + c.hlen('unacked')

    def residue(self, queue):
        """Task ids of the messages the broker still holds, by where they are."""
        c = self.client()
        out = {'ready': [], 'unacked': []}
        for k in c.scan_iter(match=f'{queue}*'):
            if c.type(k) == b'list':
                out['ready'] += [json.loads(m)['headers'].get('id') for m in c.lrange(k, 0, -1)]
        for raw in c.hvals('unacked'):
            message = json.loads(raw)[0]  # kombu stores [message, exchange, routing_key]
            if isinstance(message, (str, bytes)):
                message = json.loads(message)
            out['unacked'].append(message['headers'].get('id'))
        if c.exists('unacked_mutex'):
            out['unacked_mutex_ttl'] = c.pttl('unacked_mutex') / 1000
        return out


class DockerService:
    """A service in a docker container; ``kill`` is SIGKILL, data survives."""

    image = None
    internal_port = None
    env = {}

    def __init__(self, name, port):
        self.name, self.port = name, port

    def _docker(self, *args, check=True):
        return subprocess.run(['docker', *args], capture_output=True, text=True, check=check)

    def extra_args(self):
        return []

    def start(self, attempts=3):
        env = [a for k, v in self.env.items() for a in ('-e', f'{k}={v}')]
        for attempt in range(attempts):
            self._docker('rm', '-f', self.name, check=False)
            self._docker('run', '-d', '--name', self.name, '-p', f'127.0.0.1:{self.port}:{self.internal_port}',
                         *self.extra_args(), *env, self.image)
            try:
                return self.wait_ready()
            except TimeoutError:
                # Containers occasionally die during first boot on a loaded host
                # (seen: RabbitMQ failing to read its fresh .erlang.cookie).
                if attempt == attempts - 1 or self.running():
                    raise

    def running(self):
        out = self._docker('inspect', '-f', '{{.State.Running}}', self.name, check=False).stdout
        return out.strip() == 'true'

    def kill(self):
        self._docker('kill', self.name, check=False)

    def restart(self):
        self.kill()
        self._docker('start', self.name)
        self.wait_ready()

    def pause(self):
        self._docker('pause', self.name)

    def resume(self):
        self._docker('unpause', self.name)

    def stop(self):
        self._docker('rm', '-f', self.name, check=False)


class RabbitMQService(DockerService):
    kind = 'rabbitmq'
    image = 'rabbitmq:4.1-management'
    internal_port = 5672
    env = {'RABBITMQ_DEFAULT_USER': 'cx', 'RABBITMQ_DEFAULT_PASS': 'cx'}

    def __init__(self, name, port, mgmt_port):
        super().__init__(name, port)
        self.mgmt_port = mgmt_port

    def extra_args(self):
        return ['-p', f'127.0.0.1:{self.mgmt_port}:15672']

    def wait_ready(self):
        # Probe over HTTP only: a `docker exec rabbitmq-diagnostics` (root) during
        # boot creates .erlang.cookie before the server does, and the server
        # then dies with eacces reading it.
        _wait(lambda: self._api('/api/overview') is not None, 120, self.name, self.running)

    def _api(self, path):
        req = urllib.request.Request(f'http://127.0.0.1:{self.mgmt_port}{path}')
        req.add_header('Authorization', 'Basic ' + base64.b64encode(b'cx:cx').decode())
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.load(resp)

    def url(self, port=None):
        return f'amqp://cx:cx@127.0.0.1:{port or self.port}//'

    def depth(self, queue):
        try:
            q = self._api(f'/api/queues/%2F/{queue}')
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return 0
            raise
        return q.get('messages_ready', 0) + q.get('messages_unacknowledged', 0)

    def residue(self, queue):
        """Task ids of ready messages (peeked and requeued); unacked ones can't be listed."""
        req = urllib.request.Request(
            f'http://127.0.0.1:{self.mgmt_port}/api/queues/%2F/{queue}/get', method='POST',
            data=json.dumps({'count': 10000, 'ackmode': 'reject_requeue_true', 'encoding': 'auto'}).encode())
        req.add_header('Authorization', 'Basic ' + base64.b64encode(b'cx:cx').decode())
        req.add_header('Content-Type', 'application/json')
        with urllib.request.urlopen(req, timeout=30) as resp:
            msgs = json.load(resp)
        return {'ready': [m['properties'].get('headers', {}).get('id') for m in msgs],
                'unacked_count': self.depth(queue) - len(msgs)}


class PostgresService(DockerService):
    kind = 'postgres'
    image = 'postgres:17'
    internal_port = 5432
    env = {'POSTGRES_PASSWORD': 'cx'}

    def wait_ready(self):
        _wait(lambda: self._docker('exec', self.name, 'pg_isready', '-U', 'postgres',
                                   '-h', '127.0.0.1').returncode == 0, 60, self.name)

    def url(self, port=None):
        return f'db+postgresql+psycopg2://postgres:cx@127.0.0.1:{port or self.port}/postgres'


class MySQLService(DockerService):
    kind = 'mysql'
    image = 'mysql:8.4'
    internal_port = 3306
    env = {'MYSQL_ROOT_PASSWORD': 'cx', 'MYSQL_DATABASE': 'celery'}

    def wait_ready(self):
        _wait(lambda: self._docker('exec', self.name, 'mysql', '-h', '127.0.0.1', '-uroot', '-pcx',
                                   '-e', 'select 1', 'celery').returncode == 0, 120, self.name)

    def url(self, port=None):
        return f'db+mysql+pymysql://root:cx@127.0.0.1:{port or self.port}/celery'
