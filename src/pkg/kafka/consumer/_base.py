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

__all__ = ["_BaseConsumer"]


class _BaseConsumer:
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

    async def _validation(self, payload: bytes, model: Any) -> Any:
        try:
            if model == dict:
                return json.loads(payload.decode("utf-8"))

            if model == int:
                return int(payload.decode("utf-8"))

            if model == str:
                return payload.decode("utf-8")

            if model == bytes:
                return payload

            return model.model_validate_json(payload.decode("utf-8"))

        except Exception:
            raise ValidationException()

    async def _array_validation(self, payload: list[bytes], model: Any) -> list[Any]:
        response: list[Any] = []
        for p in payload:
            response.append(await self._validation(payload=p, model=model))

        return response

    def init_registry(
        self,
        url: str,
        key_location: str | None = None,
        certificate_location: str | None = None,
        ca_location: str | None = None,
        user_auth: str | None = None,
    ) -> None:
        self.__registry_config = {
            "url": url,
            "ssl.key.location": key_location,
            "ssl.certificate.location": certificate_location,
            "ssl.ca.location": ca_location,
            "basic.auth.user.info": user_auth,
        }
        self.registry_client = AsyncSchemaRegistryClient(self.__registry_config)

    async def _validate_json_schema(self, msg) -> bytes:
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
        return msg.value

    async def _exec(self, msg) -> None:
        raise NotImplementedError()
