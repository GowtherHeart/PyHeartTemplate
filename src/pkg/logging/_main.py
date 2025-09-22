import sys
import json

from loguru import logger


class LoggingInit:
    def __init__(self, lvl: str) -> None:
        logger.remove()

        def _json_sink(message):
            r = message.record
            out: dict[str, object] = {}
            t = r.get("time")
            # time may be a special object; use string fallback
            try:
                out["time"] = t.isoformat()  # type: ignore[attr-defined]
            except Exception:
                out["time"] = str(t)

            lvl = r.get("level")
            try:
                out["level"] = getattr(lvl, "name", str(lvl))
            except Exception:
                out["level"] = str(lvl)
            out["module"] = r.get("module")
            out["name"] = r.get("name")
            out["function"] = r.get("function")
            out["line"] = r.get("line")
            out["message"] = r.get("message")

            # Flatten extras
            for k, v in r.get("extra", {}).items():
                out[k] = v

            sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")

        logger.add(_json_sink, level=lvl, enqueue=False)

    # Retained for compatibility; unused by the JSON sink
    def format(self) -> str:
        return (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> || <lvl>{level}</lvl> || "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> || "
            "[ID-{extra[request_id]}] {message}"
        )
