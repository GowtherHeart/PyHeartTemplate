import json
from io import StringIO
from src.pkg.logging import LoggingInit
from loguru import logger


def test_logging_outputs_json(monkeypatch):
    buf = StringIO()
    monkeypatch.setattr("sys.stdout", buf)

    LoggingInit(lvl="INFO")

    logger.bind(request_id="test-req").info("hello-json")

    lines = [l for l in buf.getvalue().splitlines() if l.strip()]
    assert lines, "expected one JSON log line"
    data = json.loads(lines[-1])

    assert data["message"] == "hello-json"
    assert data["level"] == "INFO"
    assert data.get("request_id") == "test-req"
