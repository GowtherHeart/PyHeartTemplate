import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from loguru import logger

import json
from io import StringIO
from src.pkg.fastapi.middleware import MasterMiddelware, RequestLogger
from src.pkg.logging import LoggingInit


def test_request_logger_message_building(monkeypatch):
    buf = StringIO()
    monkeypatch.setattr("sys.stdout", buf)
    LoggingInit(lvl="INFO")

    RequestLogger(
        url="/x",
        method="GET",
        state="OPEN",
        status_code=200,
        content=b"body",
        time_exec="0.01",
    )

    lines = [l for l in buf.getvalue().splitlines() if l.strip()]
    assert lines, "Expected at least one log line"
    data = json.loads(lines[-1])
    assert data["state"] == "OPEN"
    assert data["time_exec"] == "0.01"
    assert data["url"] == "/x"
    assert data["method"] == "GET"
    assert data["status_code"] == 200
    assert data["content"] == "b'body'"


@pytest.mark.asyncio
async def test_master_middleware_handles_ok_and_exception():
    app = FastAPI()
    app.add_middleware(MasterMiddelware)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    @app.get("/boom")
    async def boom():
        raise RuntimeError("fail")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://x") as c:
        r1 = await c.get("/ok")
        assert r1.status_code == 200
        assert r1.json() == {"ok": True}

        r2 = await c.get("/boom")
        assert r2.status_code == 500
        # Matches middleware's JSONResponse structure
        body = r2.json()
        assert body["status_code"] == 500
        assert body["payload"] is None
        assert "exception" in body and "message" in body["exception"]
