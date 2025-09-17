import asyncio

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

    def init_batch_settings(self, timeout_ms: int, max_records: int) -> None:
        self._batch_timeout_ms = timeout_ms
        self._batch_max_records = max_records

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
            while True:
                try:
                    msg_map = await consumer.getmany(
                        timeout_ms=self._batch_timeout_ms,
                        max_records=self._batch_max_records,
                    )

                    make_tx_id()
                    with logger.contextualize(request_id=get_tx_id()):
                        logger.info("[kafka] reading msg")
                        # Aggregate messages across all topics/partitions
                        msg_array: list[bytes] = []
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

                                    except UnsupportedBytesSchemaException:
                                        ...  # TODO: make action

                                    except SchemaRegistryException:
                                        ...  # TODO: make action

                                else:
                                    payload_bytes = m.value

                                msg_array.append(payload_bytes)  # type: ignore
                                max_offset_by_tp[tp] = max(
                                    max_offset_by_tp.get(tp, -1), m.offset
                                )

                        if not msg_array:
                            logger.info("[kafka] no messages polled")
                            continue

                        try:
                            data_array = await self._array_validation(payload=msg_array, model=self.controller.model)  # type: ignore
                            await self.controller.execute(payload=data_array)  # type: ignore

                            if (
                                not self._enable_auto_commit
                                and max_offset_by_tp
                                and not self.uncommited_mode
                            ):
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
    async def _validation_schema(self, msg) -> bytes:
        return await self._validate_json_schema(msg=msg)
