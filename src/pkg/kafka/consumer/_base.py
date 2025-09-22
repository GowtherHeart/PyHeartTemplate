"""Common base utilities for Kafka consumers.

This module provides a lightweight base class used by concrete Kafka
consumers. It encapsulates shared concerns such as constructing an optional
SSL context, initializing an async Schema Registry client, validating raw
payloads into strongly typed models (including Pydantic v2), and optional
JSON Schema validation for Confluent wire-formatted messages.

No I/O with Kafka is performed here; see concrete implementations for that.
"""

import json
from ssl import SSLContext
from typing import Any

import jsonschema
from aiokafka.helpers import create_ssl_context
from confluent_kafka.schema_registry.schema_registry_client import (
    AsyncSchemaRegistryClient,
)

from src.pkg.kafka.exception import (
    SchemaRegistryException,
    UnsupportedBytesSchemaException,
    ValidationException,
)

__all__ = ["_BaseConsumer", "KafkaExceptionHandler"]


class KafkaExceptionHandler:
    async def validation(self, *args, **kwargs) -> None: ...

    async def unsupported_bytes(self, *args, **kwargs) -> None: ...

    async def schema_registry(self, *args, **kwargs) -> None: ...


class _BaseConsumer:
    """Base class with shared consumer helpers.

    Parameters mirror common Kafka client configuration. Concrete subclasses
    implement the ``exec`` loop and message handling.

    Args:
        bootstrap_server_array: Comma-separated Kafka bootstrap servers string.
        topic_array: List of topics to subscribe to.
        group_id: Consumer group id.
        sasl_username: SASL/PLAIN username.
        sasl_password: SASL/PLAIN password.
        controller: Controller instance with ``model`` and ``execute``.
        enable_auto_commit: Whether to enable Kafka auto-commit.
        cafile: Path to CA file for SSL, if any.
        certfile: Path to client certificate file, if any.
        keyfile: Path to client private key file, if any.
        client_id: Kafka client id.
        security_protocol: Kafka security protocol (e.g., ``PLAINTEXT``, ``SASL_SSL``).
        sasl_mechanism: SASL mechanism (e.g., ``PLAIN``).
        auto_offset_reset: Offset reset policy (``earliest`` or ``latest``).
        uncommited_mode: If True, skip committing offsets even when auto-commit is disabled.

    Attributes:
        registry_client: Async Schema Registry client instance when initialized.
        _ssl_context: Optional SSL context constructed from provided files.
    """

    _ssl_context: SSLContext | None = None
    registry_client: AsyncSchemaRegistryClient = None  # type: ignore

    def __init__(
        self,
        bootstrap_server_array: str,
        topic_array: list[str],
        group_id: str,
        sasl_username: str,
        sasl_password: str,
        controller: object,
        enable_auto_commit: bool = False,
        cafile: str | None = None,
        certfile: str | None = None,
        keyfile: str | None = None,
        client_id: str = "aiokafka",
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str = "PLAIN",
        auto_offset_reset: str = "latest",
        uncommited_mode: bool = False,
        exception_handler: KafkaExceptionHandler | None = None,
    ) -> None:
        self.bootstrap_server_array = bootstrap_server_array
        self.topic_array = topic_array
        self.group_id = group_id
        self.client_id = client_id
        self._sasl_username = sasl_username
        self._sasl_password = sasl_password
        self.sasl_mechanism = sasl_mechanism
        self.security_protocol = security_protocol
        self._enable_auto_commit = enable_auto_commit
        self._auto_offset_reset = auto_offset_reset
        self.uncommited_mode = uncommited_mode
        if cafile or certfile or keyfile:
            self._ssl_context = create_ssl_context(
                cafile=cafile,  # CA used to sign certificate.
                certfile=certfile,  # Signed certificate
                keyfile=keyfile,  # Private Key file of `certfile` certificate
            )

        self.controller = controller
        self.exception_handler = exception_handler

    async def _validation_exception(self, payload: bytes, model: type[Any]):
        raise ValidationException()

    async def _validation(self, payload: bytes, model: type[Any]) -> Any:
        """Convert raw payload bytes into the given model.

        Supports primitives (``dict``, ``int``, ``str``, ``bytes``) and assumes
        a Pydantic v2-style model that provides ``model_validate_json``
        otherwise.

        Args:
            payload: Raw bytes from Kafka.
            model: Target model or type to convert to.

        Returns:
            Parsed value or model instance.

        Raises:
            ValidationException: If conversion or parsing fails.
        """
        try:
            if model == dict:
                return json.loads(payload.decode("utf-8")), True

            if model == int:
                return int(payload.decode("utf-8")), True

            if model == str:
                return payload.decode("utf-8"), True

            if model == bytes:
                return payload, True

            return model.model_validate_json(payload.decode("utf-8")), True

        except Exception:
            await self._validation_exception(payload=payload, model=model)
            return payload, False

    def init_registry(
        self,
        url: str,
        key_location: str | None = None,
        certificate_location: str | None = None,
        ca_location: str | None = None,
        user_auth: str | None = None,
    ) -> None:
        """Initialize async Schema Registry client.

        Expects Confluent-style configuration. All SSL parameters are optional
        and only applied if provided.

        Args:
            url: Schema Registry base URL.
            key_location: Path to SSL key file.
            certificate_location: Path to SSL certificate file.
            ca_location: Path to CA bundle.
            user_auth: Basic auth ``"<user>:<password>"`` string.
        """
        self.__registry_config = {
            "url": url,
            "ssl.key.location": key_location,
            "ssl.certificate.location": certificate_location,
            "ssl.ca.location": ca_location,
            "basic.auth.user.info": user_auth,
        }
        self.registry_client = AsyncSchemaRegistryClient(self.__registry_config)

    async def _validate_json_schema(self, msg) -> bytes:
        """Validate a Confluent wire-formatted JSON payload against its schema.

        The value is expected to be bytes in the Confluent wire format:
        ``[magic byte=0][schema id=4 bytes][payload bytes]``. The JSON Schema is
        fetched from the configured Schema Registry using the schema id.

        Args:
            msg: Kafka message object with ``value`` bytes.

        Returns:
            The raw payload bytes (without the wire header).

        Raises:
            UnsupportedBytesSchemaException: If the message value is not in the
                expected wire format.
            SchemaRegistryException: If Schema Registry interaction fails.
        """
        if (
            isinstance(msg.value, (bytes, bytearray))
            and len(msg.value) >= 5
            and msg.value[0] == 0
        ):
            schema_id = int.from_bytes(msg.value[1:5], byteorder="big", signed=False)
            payload_bytes: bytes = msg.value[5:]  # type: ignore

        else:
            raise UnsupportedBytesSchemaException()

        try:
            resp = await self.registry_client.get_schema(schema_id=schema_id)
            data = resp.to_dict()
        except Exception:
            raise SchemaRegistryException()

        schema = json.loads(data["schema"])
        jsonschema.validate(instance=json.loads(payload_bytes), schema=schema)
        return payload_bytes

    async def _validation_schema(self, msg) -> bytes:
        """Hook for schema validation in subclasses.

        Default implementation returns the message value unchanged. Subclasses
        that rely on the Schema Registry should override this to call
        ``_validate_json_schema``.

        Args:
            msg: Kafka message object.

        Returns:
            Message value bytes.
        """
        return msg.value

    async def _exec(self, msg) -> None:
        """Placeholder for custom per-message execution logic.

        Not used by the provided concrete consumers, which instead operate in
        ``exec`` methods. Left for compatibility and extension.

        Args:
            msg: Kafka message object.

        Raises:
            NotImplementedError: Always, unless overridden by a subclass.
        """
        raise NotImplementedError()
