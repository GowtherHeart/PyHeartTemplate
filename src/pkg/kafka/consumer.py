import json
from typing import Any

from aiokafka import AIOKafkaConsumer
from loguru import logger

from src.pkg.context._main import get_tx_id, make_tx_id
from src.pkg.core.exception import CoreException

from .exception import ValidationException


class ConsumerKafka:
    def __init__(
        self,
        bootstrap_server_array: str,
        topic_array: list[str],
        group_id: str,
        sasl_username: str,
        sasl_password: str,
        controller: object,
        sasl_mechanism: str = "PLAIN",
    ) -> None:
        self.bootstrap_server_array = bootstrap_server_array
        self.topic_array = topic_array
        self.group_id = group_id
        self.__sasl_username = sasl_username
        self.__sasl_password = sasl_password
        self.sasl_mechanism = sasl_mechanism
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

    async def exec(self) -> None:
        consumer = AIOKafkaConsumer(
            *self.topic_array,
            # bootstrap_servers=",".join(self.bootstrap_server_array),
            bootstrap_servers=self.bootstrap_server_array,
            group_id=self.group_id,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.__sasl_username,
            sasl_plain_password=self.__sasl_password,
        )
        with logger.contextualize(request_id="init"):
            logger.info("RUN CONSUMER")

        await consumer.start()
        try:
            async for msg in consumer:
                make_tx_id()
                with logger.contextualize(request_id=get_tx_id()):
                    try:
                        logger.info("KafkaSTART")
                        payload = await self._validation(model=self.controller.model, payload=msg.value)  # type: ignore
                        await self.controller.execute(payload=payload)  # type: ignore
                    except CoreException as exc:
                        logger.info(f"{exc.status_code}: {exc.detail}")

                        if exc.status_code >= 500:
                            raise exc

                    except Exception as exc:
                        logger.exception(f"KAFKA_EXCEPTION: {exc}")

                    finally:
                        logger.info("KafkaEND")
        finally:
            with logger.contextualize(request_id="init"):
                logger.info("STOP CONSUMER")

            await consumer.stop()
