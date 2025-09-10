from loguru import logger

from src.config.app import get_config
from src.models.request.sample import CreatePldModel
from src.pkg.abc.controller import CliController
from src.pkg.kafka.producer import ProducerKafka
from src.usecase.sample import SampleV1US


class CreateSampleController(CliController):
    """Controller for creating a sample obj via CLI."""

    args = ["name", "content"]

    async def execute(self) -> None:
        payload = CreatePldModel(
            content=self.data.content,
            name=self.data.name,
        )
        result = await SampleV1US().create(payload=payload)
        logger.info(result)


class SendMsgController(CliController):
    args = ["content"]

    async def execute(self) -> None:
        producer = ProducerKafka(
            bootstrap_server_array=get_config().KAFKA.BOOTSTRAP_SERVER,
            sasl_username=get_config().KAFKA.USERNAME,
            sasl_password=get_config().KAFKA.PASSWORD,
            topic=get_config().KAFKA.TOPIC_ARRAY.split(",")[0],
            group_id=get_config().KAFKA.GROUP_ID,
        )
        await producer.send(msg=self.data.content)
