import json

import pytest


class DummyController:
    def __init__(self, model):
        self.model = model


class DummyModel:
    """Pydantic-v2-like shim for model_validate_json."""

    def __init__(self, a: int, b: str):
        self.a = a
        self.b = b

    @classmethod
    def model_validate_json(cls, s: str):
        data = json.loads(s)
        return cls(**data)


@pytest.mark.asyncio
async def test_base_validation_primitives_and_model():
    from src.pkg.kafka.consumer.batch import MaxBatchConsumer

    consumer = MaxBatchConsumer(
        bootstrap_server_array="kafka:9092",
        topic_array=["t"],
        group_id="g",
        sasl_username="u",
        sasl_password="p",
        controller=DummyController(model=dict),
    )

    # dict
    val, ok = await consumer._validation(b'{"x": 1}', dict)
    assert ok is True and val == {"x": 1}

    # int
    val, ok = await consumer._validation(b"42", int)
    assert ok is True and val == 42

    # str
    val, ok = await consumer._validation(b"hello", str)
    assert ok is True and val == "hello"

    # bytes
    val, ok = await consumer._validation(b"raw-bytes", bytes)
    assert ok is True and val == b"raw-bytes"

    # pydantic-like model_validate_json
    payload = json.dumps({"a": 7, "b": "z"}).encode()
    val, ok = await consumer._validation(payload, DummyModel)
    assert ok is True and isinstance(val, DummyModel) and (val.a, val.b) == (7, "z")


class _Handler:
    def __init__(self):
        self.validation_called = False
        self.unsupported_called = False
        self.schema_called = False

    async def validation(self, *_, **__):
        self.validation_called = True

    async def unsupported_bytes(self, *_, **__):
        self.unsupported_called = True

    async def schema_registry(self, *_, **__):
        self.schema_called = True


@pytest.mark.asyncio
async def test_base_validation_failure_calls_handler():
    from src.pkg.kafka.consumer.batch import MaxBatchConsumer

    handler = _Handler()
    consumer = MaxBatchConsumer(
        bootstrap_server_array="kafka:9092",
        topic_array=["t"],
        group_id="g",
        sasl_username="u",
        sasl_password="p",
        controller=DummyController(model=dict),
        exception_handler=handler,
    )

    class BadModel:
        @classmethod
        def model_validate_json(cls, s: str):
            raise ValueError("bad")

    val, ok = await consumer._validation(b"{not json}", BadModel)
    assert ok is False
    assert val == b"{not json}"
    assert handler.validation_called is True


@pytest.mark.asyncio
async def test_validate_json_schema_success_and_errors():
    from types import SimpleNamespace

    # Create a lightweight concrete instance via MaxBatchConsumer (which inherits _BaseConsumer)
    from src.pkg.kafka.consumer.batch import MaxBatchConsumer

    consumer = MaxBatchConsumer(
        bootstrap_server_array="kafka:9092",
        topic_array=["t"],
        group_id="g",
        sasl_username="u",
        sasl_password="p",
        controller=DummyController(model=dict),
    )

    # Fake Async Schema Registry client
    class FakeSchema:
        def __init__(self, schema_dict):
            self._schema_dict = schema_dict

        def to_dict(self):
            return {"schema": json.dumps(self._schema_dict)}

    class FakeRegistry:
        def __init__(self, schema_dict=None, raise_err=False):
            self.schema_dict = schema_dict
            self.raise_err = raise_err

        async def get_schema(self, schema_id: int):
            if self.raise_err:
                raise RuntimeError("registry down")
            return FakeSchema(self.schema_dict)

    # Valid wire format: magic byte 0 + 4-byte schema id + payload
    schema_id = 12
    payload = b'{"x": 1}'  # {"x": 1}
    wire = bytes([0]) + schema_id.to_bytes(4, byteorder="big", signed=False) + payload

    consumer.registry_client = FakeRegistry(
        schema_dict={
            "type": "object",
            "properties": {"x": {"type": "number"}},
            "required": ["x"],
        }
    )

    out = await consumer._validate_json_schema(SimpleNamespace(value=wire))
    assert out == payload

    # Unsupported bytes schema (missing magic byte / too short)
    from src.pkg.kafka.exception import (
        SchemaRegistryException,
        UnsupportedBytesSchemaException,
    )

    with pytest.raises(UnsupportedBytesSchemaException):
        await consumer._validate_json_schema(SimpleNamespace(value=b"bad"))

    # Registry failure path
    consumer.registry_client = FakeRegistry(raise_err=True)
    with pytest.raises(SchemaRegistryException):
        await consumer._validate_json_schema(SimpleNamespace(value=wire))
