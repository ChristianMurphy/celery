from __future__ import annotations

import pytest

from .targets import BY_NAME, TARGETS, make_app


def pytest_addoption(parser):
    parser.addoption('--backend', action='append', default=[],
                     help='Only run against these result backends.')


def pytest_generate_tests(metafunc):
    if 'target' in metafunc.fixturenames:
        wanted = metafunc.config.getoption('--backend') or [t.name for t in TARGETS]
        metafunc.parametrize('target', [BY_NAME[n] for n in wanted], ids=wanted)


@pytest.fixture
def app_factory(request, target):
    if not target.reachable():
        pytest.skip(f'{target.name} is not reachable')
    reason = target.known.get(request.node.originalname)
    if reason:
        request.applymarker(pytest.mark.xfail(reason=reason, strict=True))
    apps = []

    def factory(port=None, **extra):
        app = make_app(target, port=port, **extra)
        apps.append(app)
        if target.name == 'cassandra':
            # Only writers create the table, by design; a reader that comes
            # first gets an error rather than PENDING. Create it up front, as
            # the backend's docs ask deployments to do.
            app.backend._get_connection(write=True)
        return app

    yield factory
    for app in apps:
        try:
            app.close()
        except Exception:  # pragma: no cover
            pass
