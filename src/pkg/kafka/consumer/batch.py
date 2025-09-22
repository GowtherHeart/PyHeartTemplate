"""Kafka consumer that processes messages in batches.

MaxBatchConsumer accumulates messages across all assigned topic-partitions
using ``getmany`` with configurable timeout and max-records, validates them,
executes a controller once per batch, and conditionally commits offsets.

The RS variant (MaxBatchConsumerRS) enables Schema Registry JSON validation
for Confluent wire-formatted messages.
"""

import asyncio
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import OffsetOutOfRangeError
from loguru import logger

from src.pkg.context._main import get_tx_id, make_tx_id
from src.pkg.core.exception import CoreException
from src.pkg.kafka.exception import (
    SchemaRegistryException,
    UnsupportedBytesSchemaException,
)

from ._base import _BaseConsumer

__all__ = ["MaxBatchConsumer", "MaxBatchConsumerRS"]


class MaxBatchConsumer(_BaseConsumer):
    """Batching consumer with max-records and timeout controls."""

    def init_batch_settings(self, timeout_ms: int, max_records: int) -> None:
        """Configure batch timeout and maximum polled records.

        Args:
            timeout_ms: Maximum time in milliseconds to wait for a batch.
            max_records: Maximum number of records to retrieve per poll.
        """
        self._batch_timeout_ms = timeout_ms
        self._batch_max_records = max_records

    async def _validation_exception(self, payload: bytes, model: type[Any]):
        if self.exception_handler:
            await self.exception_handler.validation(payload=payload)

        return payload, False

    async def exec(self) -> None:
        """Run the batch polling loop and process messages.

        - Polls using ``getmany`` to collect messages across partitions
        - Optionally validates payloads via Schema Registry
        - Converts payloads into the configured controller model
        - Calls controller once per batch with the list of parsed payloads
        - Commits offsets up to the max seen per partition when auto-commit is disabled

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
            logger.info("[kafka] run")

        await consumer.start()
        try:
            while True:
                try:
                    # Poll messages across all assigned topic partitions.
                    msg_map = await consumer.getmany(
                        timeout_ms=self._batch_timeout_ms,
                        max_records=self._batch_max_records,
                    )

                    make_tx_id()
                    with logger.contextualize(request_id=get_tx_id()):
                        logger.info(f"[kafka] reading msg {msg_map.__str__()[:1000]}")

                        # Aggregate messages across all topics/partitions
                        msg_array: list[bytes] = []
                        msg_array_err: list[bytes] = []
                        max_offset_by_tp: dict[TopicPartition, int] = {}

                        for tp, messages in msg_map.items():
                            if not messages:
                                continue

                            for m in messages:
                                if self.registry_client is not None:
                                    try:
                                        payload_bytes = await self._validation_schema(
                                            msg=m
                                        )
                                        payload, ref = await self._validation(payload=payload_bytes, model=self.controller.model)  # type: ignore
                                        if ref is True:
                                            msg_array.append(payload)
                                        else:
                                            msg_array_err.append(payload)

                                    except UnsupportedBytesSchemaException:
                                        msg_array_err.append(payload)
                                        if self.exception_handler:
                                            await self.exception_handler.unsupported_bytes()

                                    except SchemaRegistryException:
                                        msg_array_err.append(payload)
                                        if self.exception_handler:
                                            await self.exception_handler.schema_registry()

                                else:
                                    payload_bytes = m.value
                                    payload, ref = await self._validation(payload=payload_bytes, model=self.controller.model)  # type: ignore
                                    if ref is True:
                                        msg_array.append(payload)
                                    else:
                                        msg_array_err.append(payload)

                                max_offset_by_tp[tp] = max(
                                    max_offset_by_tp.get(tp, -1), m.offset
                                )

                        if not msg_array and not msg_array_err:
                            logger.info("[kafka] no messages polled")
                            continue

                        try:
                            await self.controller.execute(payload=msg_array, payload_err=msg_array_err)  # type: ignore

                            if (
                                not self._enable_auto_commit
                                and max_offset_by_tp
                                and not self.uncommited_mode
                            ):
                                # Commit to the next offset after the max seen per TP.
                                commit_map = {
                                    tp: offset + 1
                                    for tp, offset in max_offset_by_tp.items()
                                }
                                await consumer.commit(offsets=commit_map)

                        except CoreException as exc:
                            logger.info(f"[kafka]{exc.status_code}: {exc.detail}")
                            if exc.status_code >= 500:
                                raise exc

                        except Exception as exc:
                            logger.exception(f"[kafka] exception: {exc}")

                        finally:
                            logger.info("[kafka] stop reading msg")

                except OffsetOutOfRangeError as err:
                    # Seek to beginning for all affected partitions
                    tps = err.args[0].keys()
                    await consumer.seek_to_beginning(*tps)
                    continue

                await asyncio.sleep(0.1)

        finally:
            await consumer.stop()


class MaxBatchConsumerRS(MaxBatchConsumer):
    """Batching consumer with Schema Registry JSON validation enabled."""

    async def _validation_schema(self, msg) -> bytes:
        return await self._validate_json_schema(msg=msg)
