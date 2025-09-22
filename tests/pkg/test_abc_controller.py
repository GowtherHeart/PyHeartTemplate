from fastapi.routing import APIRoute

from src.pkg.abc.controller import HttpController, Singleton, router


def test_singleton_metaclass_same_instance():
    class A(metaclass=Singleton):
        pass

    a1 = A()
    a2 = A()
    assert a1 is a2


def test_http_controller_builds_routes():
    class MyCtrl(HttpController):
        prefix = "/x"

        @router(path="/ping", status_code=200)
        async def get(self):  # type: ignore[override]
            return {"pong": True}

    ctrl = MyCtrl()
    # Ensure exactly one route is registered for GET /x/ping
    paths = [r.path for r in ctrl.router.routes if isinstance(r, APIRoute)]
    assert "/x/ping" in paths
