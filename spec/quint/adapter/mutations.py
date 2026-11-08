"""Bugs planted in celery's code, to check that the replay catches them.

Each mutation is a textual edit of one function's source (or one Lua
script), applied in-process before the backends are created:

    python -m adapter.replay_results --backend redis --mutate kv_no_success_check ...
"""
from __future__ import annotations

import importlib
import inspect
import textwrap

# name -> (module, attribute path, old text, new text, what it breaks)
MUTATIONS = {
    'kv_no_success_check': (
        'celery.backends.base', 'BaseKeyValueStoreBackend._store_result',
        "if current_meta['status'] == states.SUCCESS:",
        "if False:",
        'base.py:1369 keep the first SUCCESS (read-then-write backends)'),
    'mongo_no_filter': (
        'celery.backends.mongodb', 'MongoBackend._store_result',
        "query = {'_id': task_id, 'status': {'$ne': states.SUCCESS}}",
        "query = {'_id': task_id}",
        'mongodb.py:207 the $ne SUCCESS filter'),
    'sql_no_success_check': (
        'celery.backends.database', 'DatabaseBackend._store_result',
        "elif task.status == states.SUCCESS:",
        "elif False:",
        'database/__init__.py:186 keep the first SUCCESS'),
    'sql_no_row_lock': (
        'celery.backends.database', 'DatabaseBackend._store_result',
        "task = self._query_task(session, task_id, for_update=True)",
        "task = task",
        'database/__init__.py:180 re-read under the row lock'),
    'redis_lua_no_dedupe': (
        'celery.backends.redis', 'CHORD_PART_RETURN_LUA',
        "if redis.call('SISMEMBER', dkey, member) == 0 then",
        "if true then",
        'redis.py:111 count each header once (celery PR 10919)'),
    'redis_lua_claim_no_nx': (
        'celery.backends.redis', 'CHORD_PART_RETURN_LUA',
        "redis.call('SET', ckey, claim_token, 'NX', 'EX', claim_expires)",
        "redis.call('SET', ckey, claim_token, 'EX', claim_expires)",
        'redis.py:139 one dispatch claim (celery PR 10998)'),
    'redis_lua_no_done_marker': (
        'celery.backends.redis', 'CHORD_PART_RETURN_LUA',
        "if done_marker == 'done' then",
        "if false then",
        'redis.py:104 late duplicates stop at the done marker'),
    'redis_cleanup_any_owner': (
        'celery.backends.redis', 'CHORD_CLEANUP_LUA',
        "if redis.call('GET', ckey) == token then",
        "if true then",
        'redis.py:160 only the claim owner cleans up (celery issue 10997)'),
    'redis_extend_any_owner': (
        'celery.backends.redis', 'CHORD_CLAIM_EXTEND_LUA',
        "if redis.call('GET', ckey) == token then",
        "if true then",
        'redis.py:176 only the claim owner extends'),
    'kv_count_early': (
        'celery.backends.base', 'BaseKeyValueStoreBackend.on_chord_part_return',
        "elif val == size:",
        "elif val == size - 1:",
        'base.py:1465 dispatch only when the counter hits the size'),
    # val >= size would be an equivalent mutant: val > size is tested first.
    'kv_keep_counter': (
        'celery.backends.base', 'BaseKeyValueStoreBackend.on_chord_part_return',
        "self.delete(key)\n",
        "pass\n",
        'base.py:1495 drop the counter when the chord is done'),
    'worker_lost_ignores_success': (
        'celery.worker.request', 'Request.on_failure',
        "success_meta = self._success_was_stored() if is_worker_lost else False",
        "success_meta = False",
        'request.py:714 celery PR 10752'),
}


def apply(name: str) -> None:
    modname, path, old, new, _ = MUTATIONS[name]
    mod = importlib.import_module(modname)
    owner_name, _, attr = path.rpartition('.')
    if not owner_name:
        text = getattr(mod, attr)
        assert old in text, f'{name}: text not found in {path}'
        setattr(mod, attr, text.replace(old, new))
        return
    owner = getattr(mod, owner_name)
    func = owner.__dict__[attr]
    src = textwrap.dedent(inspect.getsource(func))
    assert old in src, f'{name}: text not found in {path}'
    src = src.replace(old, new)
    ns: dict = {}
    exec(compile(src, f'<mutant {name}>', 'exec'), mod.__dict__, ns)
    setattr(owner, attr, ns[attr])
