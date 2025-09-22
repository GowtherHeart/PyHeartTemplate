"""Single-message Kafka consumer implementations.

ConsumerKafka processes messages one-by-one, validating and handing each to a
controller. The RS variant (ConsumerKafkaRS) enables Schema Registry JSON
validation for Confluent wire-formatted messages.
"""

from aiokafka import AIOKafkaConsumer, TopicPartition
from loguru import logger

from src.pkg.context._main import get_tx_id, make_tx_id
from src.pkg.core.exception import CoreException
from src.pkg.kafka.exception import (
    SchemaRegistryException,
    UnsupportedBytesSchemaException,
)

from ._base import _BaseConsumer

__all__ = ["ConsumerKafka", "ConsumerKafkaRS"]


class ConsumerKafka(_BaseConsumer):
    """Simple per-message consumer."""

    async def exec(self) -> None:
        """Run the consume loop and process each message.

        - Optionally validates payloads via Schema Registry
        - Converts the payload into the configured controller model
        - Calls controller per message
        - Commits the message offset when auto-commit is disabled

        Returns:
            None
        """
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
            logger.bind(event="kafka_run").info("kafka")

        await consumer.start()
        try:
            async for msg in consumer:
                make_tx_id()
                with logger.contextualize(request_id=get_tx_id()):
                    logger.bind(
                        event="kafka_reading_msg",
                        topic=msg.topic,
                        partition=msg.partition,
                        offset=msg.offset,
                    ).info("kafka")
                    try:
                        if self.registry_client is not None:
                            try:
                                payload_bytes = await self._validation_schema(msg=msg)

                            except UnsupportedBytesSchemaException:
                                ...  # TODO: make action

                            except SchemaRegistryException:
                                ...  # TODO: make action

                        else:
                            payload_bytes = msg.value

                        # Convert raw bytes to the controller's model instance/value.
                        payload = await self._validation(model=self.controller.model, payload=payload_bytes)  # type: ignore
                        await self.controller.execute(payload=payload)  # type: ignore

                        if not self._enable_auto_commit and not self.uncommited_mode:
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
                        logger.bind(event="kafka_stop_reading_msg").info("kafka")
        finally:
            with logger.contextualize(request_id="meta"):
                logger.bind(event="kafka_stop_consumer").info("kafka")

            await consumer.stop()


class ConsumerKafkaRS(ConsumerKafka):
    """Per-message consumer with Schema Registry JSON validation enabled."""

    async def _validation_schema(self, msg) -> bytes:
        return await self._validate_json_schema(msg=msg)
