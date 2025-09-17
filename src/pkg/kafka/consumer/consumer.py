import json

import jsonschema
from aiokafka import AIOKafkaConsumer, TopicPartition
from confluent_kafka.schema_registry.schema_registry_client import (
    AsyncSchemaRegistryClient,
)
from loguru import logger

from src.pkg.context._main import get_tx_id, make_tx_id
from src.pkg.core.exception import CoreException

from ._base import _BaseConsumer

__all__ = ["ConsumerKafka", "RegistyConsumerKafka"]


class _Consumer(_BaseConsumer):
    async def exec(self) -> None:
        consumer = AIOKafkaConsumer(
            *self.topic_array,
            bootstrap_servers=self.bootstrap_server_array,
            group_id=self.group_id,
            client_id=self.client_id,
            sasl_plain_username=self._sasl_username,
            sasl_plain_password=self._sasl_password,
            enable_auto_commit=self._enable_auto_commit,
            sasl_mechanism=self.sasl_mechanism,
            ssl_context=self._ssl_context,
            security_protocol=self.security_protocol,
            auto_offset_reset=self._auto_offset_reset,
        )
        with logger.contextualize(request_id="init"):
            logger.info("[kafka] run")

        await consumer.start()
        try:
            async for msg in consumer:
                make_tx_id()
                with logger.contextualize(request_id=get_tx_id()):
                    logger.info("[kafka] reading msg")
                    try:
                        await self._exec(msg=msg)
                        # Commit manually when auto-commit is disabled.
                        if not self._enable_auto_commit:
                            tp = TopicPartition(
                                topic=msg.topic, partition=msg.partition
                            )
                            await consumer.commit(offsets={tp: msg.offset + 1})

                    except CoreException as exc:
                        logger.info(f"[kafka]{exc.status_code}: {exc.detail}")
                        if exc.status_code >= 500:
                            raise exc

                    except Exception as exc:
                        logger.exception(f"[kafka] exception: {exc}")

                    finally:
                        logger.info("[kafka] stop reading msg")
        finally:
            with logger.contextualize(request_id="meta"):
                logger.info("[kafka] stop consumer")

            await consumer.stop()


class ConsumerKafka(_Consumer):
    async def _exec(self, msg) -> None:
        payload = await self._validation(model=self.controller.model, payload=msg.value)  # type: ignore
        await self.controller.execute(payload=payload)  # type: ignore


class RegistyConsumerKafka(_Consumer):
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

    async def _exec(self, msg) -> None:
        raw = msg.value
        if isinstance(raw, (bytes, bytearray)) and len(raw) >= 5 and raw[0] == 0:
            schema_id = int.from_bytes(raw[1:5], byteorder="big", signed=False)
            payload_bytes = raw[5:]

        else:
            return

        try:
            resp = await self.registry_client.get_schema(schema_id=schema_id)
            data = resp.to_dict()
        except Exception:
            return

        schema = json.loads(data["schema"])
        jsonschema.validate(instance=json.loads(payload_bytes), schema=schema)

        payload = await self._validation(model=self.controller.model, payload=payload_bytes)  # type: ignore
        await self.controller.execute(payload=payload)  # type: ignore
