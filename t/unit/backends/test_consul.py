from unittest.mock import Mock

import pytest

from celery.backends.consul import ConsulBackend
from celery.exceptions import BackendStoreError

consul = pytest.importorskip('consul')


class test_ConsulBackend:

    def setup_method(self):
        self.backend = ConsulBackend(
            app=self.app, url='consul://localhost:800')

    def test_supports_autoexpire(self):
        assert self.backend.supports_autoexpire

    def test_consul_consistency(self):
        assert self.backend.consistency == 'consistent'

    def test_get(self):
        index = 100
        data = {'Key': 'test-consul-1', 'Value': 'mypayload'}
        self.backend.one_client = Mock(name='c.client')
        self.backend.one_client.kv.get.return_value = (index, data)
        assert self.backend.get(data['Key']) == 'mypayload'

    def test_set(self):
        self.backend.one_client = Mock(name='c.client')
        self.backend.one_client.kv.get.return_value = (1, None)
        self.backend.one_client.session.create.return_value = 'c8dfa770-4ea3-2ee9-d141-98cf0bfe9c59'
        self.backend.one_client.kv.put.return_value = True
        assert self.backend.set('Key', 'Value') is True
        self.backend.one_client.kv.put.assert_called_once_with(
            key='Key', value='Value',
            acquire='c8dfa770-4ea3-2ee9-d141-98cf0bfe9c59')

    def test_set_reuses_the_session_holding_the_key(self):
        # Consul refuses a put that acquires a key with a session other than
        # the one holding it, so a task's later states would be lost.
        client = self.backend.one_client = Mock(name='c.client')
        client.kv.get.return_value = (1, {'Key': 'Key', 'Session': 'held'})
        client.kv.put.return_value = True
        assert self.backend.set('Key', 'SUCCESS') is True
        client.session.renew.assert_called_once_with('held')
        client.session.create.assert_not_called()
        client.kv.put.assert_called_once_with(
            key='Key', value='SUCCESS', acquire='held')

    def test_set_replaces_an_expired_holder(self):
        client = self.backend.one_client = Mock(name='c.client')
        client.kv.get.return_value = (1, {'Key': 'Key', 'Session': 'gone'})
        client.session.renew.side_effect = consul.NotFound()
        client.session.create.return_value = 'new'
        client.kv.put.return_value = True
        assert self.backend.set('Key', 'Value') is True
        client.kv.put.assert_called_once_with(
            key='Key', value='Value', acquire='new')

    def test_set_retries_when_another_session_takes_the_key(self):
        client = self.backend.one_client = Mock(name='c.client')
        client.kv.get.side_effect = [
            (1, None),
            (2, {'Key': 'Key', 'Session': 'other'}),
        ]
        client.session.create.return_value = 'mine'
        client.kv.put.side_effect = [False, True]
        assert self.backend.set('Key', 'Value') is True
        assert client.kv.put.call_args_list[-1].kwargs['acquire'] == 'other'

    def test_set_raises_when_the_key_cannot_be_written(self):
        client = self.backend.one_client = Mock(name='c.client')
        client.kv.get.return_value = (1, {'Key': 'Key', 'Session': 'other'})
        client.kv.put.return_value = False
        with pytest.raises(BackendStoreError):
            self.backend.set('Key', 'Value')
        assert client.kv.put.call_count == self.backend._max_set_attempts

    def test_store_result_after_started(self):
        # A task tracked as STARTED must still reach SUCCESS.
        store = {}

        def put(key, value, acquire):
            holder = store.get(key, (None, None))[1]
            if holder not in (None, acquire):
                return False
            store[key] = (value, acquire)
            return True

        def get(key):
            if key not in store:
                return 1, None
            value, session = store[key]
            return 1, {'Key': key, 'Value': value, 'Session': session}

        client = self.backend.one_client = Mock(name='c.client')
        client.kv.put.side_effect = put
        client.kv.get.side_effect = get
        client.session.create.side_effect = ['s1', 's2']
        self.backend.mark_as_started('id1')
        self.backend.mark_as_done('id1', 42)
        assert self.backend.get_task_meta('id1', cache=False)['status'] == 'SUCCESS'
        assert self.backend.get_task_meta('id1', cache=False)['result'] == 42

    def test_delete(self):
        self.backend.one_client = Mock(name='c.client')
        self.backend.one_client.kv.delete.return_value = True
        assert self.backend.delete('Key') is True

    def test_index_bytes_key(self):
        key = 'test-consul-2'
        assert self.backend._key_to_consul_key(key) == key
        assert self.backend._key_to_consul_key(key.encode('utf-8')) == key
