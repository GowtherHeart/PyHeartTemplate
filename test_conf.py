import os
from typing import Optional

from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringDeserializer,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is not None and isinstance(v, str):
        v = v.strip()
    return v if v else default


def _build_schema_registry_client() -> SchemaRegistryClient:
    # Tries common env names. Example:
    #  SCHEMA_REGISTRY__URL=http://localhost:8081
    #  SCHEMA_REGISTRY__USERNAME=foo
    #  SCHEMA_REGISTRY__PASSWORD=bar
    url = (
        _env("SCHEMA_REGISTRY__URL")
        or _env("SCHEMA_REGISTRY_URL")
        or "http://localhost:8081"
    )

    username = _env("SCHEMA_REGISTRY__USERNAME") or _env("SCHEMA_REGISTRY_USERNAME")
    password = _env("SCHEMA_REGISTRY__PASSWORD") or _env("SCHEMA_REGISTRY_PASSWORD")

    conf: dict[str, str] = {"url": url}
    if username and password:
        conf["basic.auth.user.info"] = f"{username}:{password}"

    return SchemaRegistryClient(conf)


def _resolve_json_schema(schema_registry: SchemaRegistryClient) -> str:
    # Priority: explicit schema string -> subject lookup -> permissive fallback
    schema_str = _env("KAFKA__VALUE_SCHEMA_STR") or _env("VALUE_SCHEMA_STR")
    if schema_str:
        return schema_str

    subject = (
        _env("KAFKA__VALUE_SUBJECT")
        or _env("SCHEMA_REGISTRY__VALUE_SUBJECT")
        or _env("VALUE_SUBJECT")
    )
    if subject:
        # Fetch latest schema for the subject from Schema Registry
        latest = schema_registry.get_latest_version(subject)
        return latest.schema.schema_str

    # Fallback permissive JSON Schema for arbitrary objects
    return '{"title": "GenericPayload", "type": "object", "additionalProperties": true}'


def make_json_consumer():
    """Create a Confluent Kafka consumer with JSON Schema deserialization.

    Env vars used (examples in example.env):
      - KAFKA__BOOTSTRAP_SERVER (e.g. "localhost:9092")
      - KAFKA__GROUP_ID (e.g. "my-group")
      - KAFKA__USERNAME (optional for SASL)
      - KAFKA__PASSWORD (optional for SASL)
      - KAFKA__SECURITY_PROTOCOL (default: SASL_PLAINTEXT if username is set, else PLAINTEXT)
      - KAFKA__SASL_MECHANISM (default: PLAIN)
      - KAFKA__AUTO_OFFSET_RESET (default: earliest)

    Schema Registry (optional):
      - SCHEMA_REGISTRY__URL (default: http://localhost:8081)
      - SCHEMA_REGISTRY__USERNAME / SCHEMA_REGISTRY__PASSWORD (if auth is required)
      - VALUE_SCHEMA_STR (inline schema) or VALUE_SUBJECT (to fetch latest)
    """

    bootstrap = _env("KAFKA__BOOTSTRAP_SERVER", "localhost:9092")
    group_id = _env("KAFKA__GROUP_ID", "test-group")
    username = _env("KAFKA__USERNAME")
    password = _env("KAFKA__PASSWORD")
    auto_offset = _env("KAFKA__AUTO_OFFSET_RESET", "earliest")

    # Default protocol based on presence of SASL credentials
    security_protocol = _env(
        "KAFKA__SECURITY_PROTOCOL",
        "SASL_PLAINTEXT" if username else "PLAINTEXT",
    )
    sasl_mechanism = _env("KAFKA__SASL_MECHANISM", "PLAIN")

    # Schema Registry client + JSON deserializer
    sr_client = _build_schema_registry_client()
    schema_str = _resolve_json_schema(sr_client)
    json_deserializer = JSONDeserializer(
        schema_str=schema_str,
        schema_registry_client=sr_client,
        from_dict=lambda obj, ctx: obj,  # keep as dict
    )

    conf: dict[str, object] = {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": auto_offset,
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": json_deserializer,
    }

    if security_protocol:
        conf["security.protocol"] = security_protocol
    if username and password:
        conf["sasl.mechanisms"] = sasl_mechanism
        conf["sasl.username"] = username
        conf["sasl.password"] = password

    consumer = DeserializingConsumer(conf)
    return consumer, json_deserializer


def run():
    """Example consumption loop. Requires a running Kafka and Schema Registry.

    Configure topics with KAFKA__TOPIC_ARRAY (comma-separated).
    """
    topics = [
        t.strip() for t in (_env("KAFKA__TOPIC_ARRAY", "").split(",")) if t.strip()
    ]
    if not topics:
        topics = ["core"]  # sensible default for local testing

    consumer, _ = make_json_consumer()
    consumer.subscribe(topics)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # Delivery or broker error
                print(f"Kafka error: {msg.error()}")
                continue

            # Value is already deserialized (dict)
            value = msg.value()
            key = msg.key()  # if key was a string, it is already deserialized too
            print(
                f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} key={key} value={value}"
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
