import os
import json
import asyncio
from typing import Optional, Tuple

from aiokafka import AIOKafkaConsumer
from loguru import logger

import httpx
from jsonschema import validators


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is not None and isinstance(v, str):
        v = v.strip()
    return v if v else default


async def _fetch_schema_from_registry_subject(
    subject: str, base_url: str, username: Optional[str], password: Optional[str]
) -> Optional[dict]:
    url = f"{base_url.rstrip('/')}/subjects/{subject}/versions/latest"
    auth = (username, password) if (username and password) else None
    async with httpx.AsyncClient() as client:
        r = await client.get(url, auth=auth, timeout=10)
        r.raise_for_status()
        payload = r.json()
        schema_str = payload.get("schema")
        if not schema_str:
            return None
        try:
            return json.loads(schema_str)
        except json.JSONDecodeError:
            return None


async def _fetch_schema_from_registry_id(
    schema_id: int, base_url: str, username: Optional[str], password: Optional[str]
) -> Optional[dict]:
    url = f"{base_url.rstrip('/')}/schemas/ids/{schema_id}"
    auth = (username, password) if (username and password) else None
    async with httpx.AsyncClient() as client:
        r = await client.get(url, auth=auth, timeout=10)
        r.raise_for_status()
        payload = r.json()
        schema_str = payload.get("schema")
        if not schema_str:
            return None
        try:
            return json.loads(schema_str)
        except json.JSONDecodeError:
            return None


async def _resolve_json_schema() -> dict:
    # Inline schema has the highest priority
    inline_schema = _env("KAFKA__VALUE_SCHEMA_STR") or _env("VALUE_SCHEMA_STR")
    if inline_schema:
        try:
            return json.loads(inline_schema)
        except json.JSONDecodeError:
            logger.warning("VALUE_SCHEMA_STR is not valid JSON; using fallback schema")

    # Try fetching by subject from Schema Registry
    subject = _env("KAFKA__VALUE_SUBJECT") or _env("SCHEMA_REGISTRY__VALUE_SUBJECT") or _env("VALUE_SUBJECT")
    if subject:
        base_url = _env("SCHEMA_REGISTRY__URL", "http://localhost:8081") or "http://localhost:8081"
        username = _env("SCHEMA_REGISTRY__USERNAME") or _env("SCHEMA_REGISTRY_USERNAME")
        password = _env("SCHEMA_REGISTRY__PASSWORD") or _env("SCHEMA_REGISTRY_PASSWORD")
        try:
            fetched = await _fetch_schema_from_registry_subject(subject, base_url, username, password)
            if fetched:
                return fetched
        except Exception as exc:
            logger.warning(f"Schema Registry fetch failed: {exc}; using fallback schema")

    # Fallback permissive schema
    return {"title": "GenericPayload", "type": "object", "additionalProperties": True}


async def make_aiokafka_json_consumer() -> Tuple[AIOKafkaConsumer, Optional[object]]:
    """Create an AIOKafka consumer that decodes JSON and validates via JSON Schema.

    Kafka env vars:
      - KAFKA__BOOTSTRAP_SERVER (default: localhost:9092)
      - KAFKA__GROUP_ID (default: test-group)
      - KAFKA__USERNAME / KAFKA__PASSWORD (optional SASL)
      - KAFKA__SECURITY_PROTOCOL (default: SASL_PLAINTEXT if username set else PLAINTEXT)
      - KAFKA__SASL_MECHANISM (default: PLAIN)
      - KAFKA__AUTO_OFFSET_RESET (default: earliest)

    Schema Registry env (optional):
      - SCHEMA_REGISTRY__URL (default: http://localhost:8081)
      - SCHEMA_REGISTRY__USERNAME / SCHEMA_REGISTRY__PASSWORD
      - VALUE_SCHEMA_STR or VALUE_SUBJECT / KAFKA__VALUE_SUBJECT
    """
    bootstrap = _env("KAFKA__BOOTSTRAP_SERVER", "localhost:9092")
    group_id = _env("KAFKA__GROUP_ID", "test-group")
    username = _env("KAFKA__USERNAME")
    password = _env("KAFKA__PASSWORD")
    auto_offset = _env("KAFKA__AUTO_OFFSET_RESET", "earliest")
    security_protocol = _env("KAFKA__SECURITY_PROTOCOL", "SASL_PLAINTEXT" if username else "PLAINTEXT")
    sasl_mechanism = _env("KAFKA__SASL_MECHANISM", "PLAIN")

    # Prepare JSON Schema validator (optional)
    schema_dict = await _resolve_json_schema()
    try:
        Validator = validators.validator_for(schema_dict)
        Validator.check_schema(schema_dict)
        validator = Validator(schema_dict)
    except Exception as exc:
        logger.warning(f"Invalid JSON Schema, skipping validation: {exc}")
        validator = None

    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset=auto_offset,  # type: ignore[arg-type]
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=username,
        sasl_plain_password=password,
    )
    return consumer, validator


async def run() -> None:
    topics_env = _env("KAFKA__TOPIC_ARRAY", "") or ""
    topics = [t.strip() for t in topics_env.split(",") if t.strip()] or ["core"]

    consumer, validator = await make_aiokafka_json_consumer()

    # Schema Registry configuration to resolve schema by ID from Confluent wire format
    sr_url = _env("SCHEMA_REGISTRY__URL", "http://localhost:8081") or "http://localhost:8081"
    sr_username = _env("SCHEMA_REGISTRY__USERNAME") or _env("SCHEMA_REGISTRY_USERNAME")
    sr_password = _env("SCHEMA_REGISTRY__PASSWORD") or _env("SCHEMA_REGISTRY_PASSWORD")

    # Simple in-memory cache for schema ID -> validator
    schema_validator_cache: dict[int, Optional[object]] = {}

    with logger.contextualize(request_id="init"):
        logger.info("RUN AIO CONSUMER")

    await consumer.start()
    try:
        await consumer.subscribe(topics)
        async for msg in consumer:
            try:
                raw = msg.value
                data = None

                # Handle Confluent wire format: magic byte (0) + 4-byte schema id (big-endian)
                if isinstance(raw, (bytes, bytearray)) and len(raw) >= 5 and raw[0] == 0:
                    schema_id = int.from_bytes(raw[1:5], byteorder="big", signed=False)
                    payload_bytes = raw[5:]

                    # Decode JSON payload
                    data = json.loads(payload_bytes.decode("utf-8"))

                    # Resolve validator for this schema id (if possible)
                    if schema_id not in schema_validator_cache:
                        try:
                            schema_dict = await _fetch_schema_from_registry_id(schema_id, sr_url, sr_username, sr_password)
                            if schema_dict:
                                Validator = validators.validator_for(schema_dict)
                                Validator.check_schema(schema_dict)
                                schema_validator_cache[schema_id] = Validator(schema_dict)
                            else:
                                schema_validator_cache[schema_id] = None
                        except Exception as exc:
                            logger.warning(f"Failed to fetch/prepare validator for schema_id={schema_id}: {exc}")
                            schema_validator_cache[schema_id] = None

                    if schema_validator_cache[schema_id] is not None:
                        schema_validator_cache[schema_id].validate(data)  # type: ignore[union-attr]

                else:
                    # Fallback: plain JSON (no Confluent framing)
                    if isinstance(raw, (bytes, bytearray)):
                        raw = raw.decode("utf-8")
                    data = json.loads(raw) if isinstance(raw, str) else raw

                    # Validate against globally resolved schema (if provided)
                    if validator is not None:
                        validator.validate(data)

                logger.info(
                    f"topic={msg.topic} partition={msg.partition} offset={msg.offset} key={msg.key} value={data}"
                )
            except json.JSONDecodeError as exc:
                logger.warning(f"JSON decode failed: {exc}")
            except Exception as exc:
                logger.exception(f"Message handling failed: {exc}")
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        with logger.contextualize(request_id="init"):
            logger.info("STOP AIO CONSUMER")
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(run())
