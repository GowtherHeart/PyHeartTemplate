import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from loguru import logger

from src.pkg.fastapi.middleware import MasterMiddelware, RequestLogger


def test_request_logger_message_building():
    messages: list[str] = []
    i = logger.add(lambda m: messages.append(m))
    try:
        RequestLogger(
            url="/x",
            method="GET",
            state="OPEN",
            status_code=200,
            content=b"body",
            time_exec="0.01",
        )
    finally:
        logger.remove(i)

    assert messages, "Expected at least one log message to be captured"
    msg = messages[-1]
    assert "[STATE-OPEN]" in msg
    assert "[Time-0.01]" in msg
    assert "URL: /x, Method: GET" in msg
    assert ", Status-Code: 200" in msg
    assert ", Content: b'body'" in msg


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
