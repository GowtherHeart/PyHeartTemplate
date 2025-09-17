import json
from ssl import SSLContext
from typing import Any

from aiokafka.helpers import create_ssl_context

from src.pkg.kafka.exception import ValidationException

__all__ = ["_BaseConsumer"]


class _BaseConsumer:
    _ssl_context: SSLContext | None = None

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

            return model(**json.loads(payload.decode("utf-8")))

        except Exception:
            raise ValidationException()

    async def _array_validation(self, payload: list[bytes], model: Any) -> list[Any]:
        response: list[Any] = []
        for p in payload:
            response.append(await self._validation(payload=p, model=model))

        return response

    async def _exec(self, msg) -> None:
        raise NotImplementedError()
