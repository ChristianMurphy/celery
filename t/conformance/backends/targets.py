"""Result backends the conformance suite can run against.

Each target names a backend and how to reach a local instance of the service
it needs. A target whose service isn't reachable is skipped, so the suite runs
against whatever is up. ``docker-compose.yml`` next to this file starts all of
them.
"""
from __future__ import annotations

import os
import socket
import tempfile
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from celery import Celery

AZURITE = (
    'azureblockblob://DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;'
    'AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/'
    'K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;'
)


@dataclass(frozen=True)
class Target:
    name: str
    url: str | Callable[[], str]
    port: int | None = None
    conf: dict[str, Any] = field(default_factory=dict)
    #: Also never lets an unready state replace a ready one (see model.py).
    ready_sticky: bool = False
    #: Known gaps, by test name, each with the upstream report that tracks it.
    #: They run as strict xfails, so a fix shows up as an XPASS.
    known: dict[str, str] = field(default_factory=dict)

    def reachable(self) -> bool:
        if self.port is None:
            return True
        try:
            with socket.create_connection(('127.0.0.1', self.port), timeout=1):
                return True
        except OSError:
            return False

    def resolve_url(self) -> str:
        return self.url() if callable(self.url) else self.url


_tmp = tempfile.mkdtemp(prefix='celery-conformance-')
_run = uuid.uuid4().hex[:8]


def _sqlite_url() -> str:
    return f'db+sqlite:///{_tmp}/results-{_run}.sqlite'


def _file_url() -> str:
    path = os.path.join(_tmp, 'files')
    os.makedirs(path, exist_ok=True)
    return f'file://{path}'


TARGETS = [
    Target('redis', 'redis://127.0.0.1:6379/0', 6379),
    Target('mongodb', f'mongodb://127.0.0.1:27017/conformance_{_run}', 27017),
    Target('postgresql', 'db+postgresql+psycopg2://postgres:pw@127.0.0.1:5432/celery', 5432),
    Target('mysql', 'db+mysql+pymysql://root:pw@127.0.0.1:3306/celery', 3306),
    Target('sqlite', _sqlite_url),
    Target('filesystem', _file_url, known={
        'test_keeps_first_success': 'forget() of a missing result raises '
                                    'FileNotFoundError (celery PR 10678)',
    }),
    Target('memcached', 'cache+memcached://127.0.0.1:11211/', 11211),
    Target('couchdb', f'couchdb://admin:pw@127.0.0.1:5984/conformance_{_run}', 5984),
    Target('consul', 'consul://127.0.0.1:8500/', 8500, known={
        'test_keeps_first_success': 'every write after the first is dropped '
                                    '(fix: branch fix/consul-lost-writes)',
    }),
    Target('azureblockblob', AZURITE, 10000,
           conf={'azureblockblob_container_name': f'conformance-{_run}'}),
    Target('dynamodb', f'dynamodb://key:secret@localhost:8000/conformance_{_run}', 8000),
    Target('elasticsearch', f'elasticsearch://127.0.0.1:9200/conformance_{_run}', 9200,
           ready_sticky=True),
    Target('cassandra', 'cassandra://', 9042, conf={
        'cassandra_servers': ['127.0.0.1'],
        'cassandra_keyspace': 'celery',
        'cassandra_table': f'conformance_{_run}',
        'cassandra_read_consistency': 'ONE',
        'cassandra_write_consistency': 'ONE',
    }),
    Target('arangodb', 'arangodb://root:pw@127.0.0.1:8529/_system/conformance', 8529),
]

RACE = ('reads what is stored, then writes in a separate step, so a late '
        'duplicate run can replace a SUCCESS')
for _name in ('redis', 'memcached', 'couchdb', 'azureblockblob', 'dynamodb',
              'cassandra', 'arangodb'):
    _t = next(t for t in TARGETS if t.name == _name)
    _t.known['test_first_success_survives_a_late_writer'] = RACE

BY_NAME = {t.name: t for t in TARGETS}


def make_app(target: Target, port: int | None = None, **extra) -> Celery:
    """Build a separate app (and so a separate client) for ``target``.

    ``port`` reaches the service through another port, such as a proxy's.
    """
    url = target.resolve_url()
    conf = dict(target.conf)
    if port is not None and target.port is not None:
        url = url.replace(f':{target.port}', f':{port}')
        if target.name == 'cassandra':
            conf['cassandra_port'] = port
    app = Celery(f'conformance-{target.name}', set_as_current=False)
    app.conf.update(
        result_backend=url,
        result_extended=True,
        result_backend_always_retry=False,
        **conf,
        **extra,
    )
    return app
