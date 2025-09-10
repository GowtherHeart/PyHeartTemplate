import asyncio
from src.pkg.abc.cmd import Cmd
from src.pkg.kafka.consumer import ConsumerKafka
from src.config.app import ConfigName, get_config
from src.controllers.sample.kafka import SampleController


class CoreConsumerCmd(Cmd):
    name = "CoreConsumer"

    config_array = [
        ConfigName.KAFKA,
        ConfigName.POSTGRES,
        ConfigName.LOGGING,
    ]

    def run(self):
        controller = SampleController()
        consumer = ConsumerKafka(
            bootstrap_server_array=get_config().KAFKA.BOOTSTRAP_SERVER,
            sasl_username=get_config().KAFKA.USERNAME,
            sasl_password=get_config().KAFKA.PASSWORD,
            topic_array=get_config().KAFKA.TOPIC_ARRAY.split(","),
            group_id=get_config().KAFKA.GROUP_ID,
            controller=controller,
        )

        asyncio.run(consumer.exec())
