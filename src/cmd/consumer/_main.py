import asyncio

from loguru import logger

from src.config.app import ConfigName, get_config
from src.controllers.sample.kafka import SampleController
from src.pkg.abc.cmd import Cmd
from src.pkg.driver.postgres._main import PostgresDriver
from src.pkg.driver.query import inject as db_inject
from src.pkg.kafka.consumer import MaxBatchConsumer
from src.repository import _startup as _startup_repo
from src.repository import sample as sample_repo


class CoreConsumerCmd(Cmd):
    name = "CoreConsumer"

    config_array = [
        ConfigName.KAFKA,
        ConfigName.POSTGRES,
        ConfigName.LOGGING,
    ]

    def run(self):

        driver = PostgresDriver(
            host=get_config().POSTGRES.HOST,
            port=get_config().POSTGRES.PORT,
            username=get_config().POSTGRES.USERNAME,
            password=get_config().POSTGRES.PASSWORD,
            db=get_config().POSTGRES.DB,
        )
        with logger.contextualize(request_id="init"):
            db_inject(_startup_repo, driver)
            db_inject(sample_repo, driver)

        controller = SampleController()
        consumer = MaxBatchConsumer(
            bootstrap_server_array=get_config().KAFKA.BOOTSTRAP_SERVER,
            sasl_username=get_config().KAFKA.USERNAME,
            sasl_password=get_config().KAFKA.PASSWORD,
            topic_array=get_config().KAFKA.TOPIC_ARRAY.split(","),
            group_id=get_config().KAFKA.GROUP_ID,
            controller=controller,
        )
        consumer.init_batch_settings(
            timeout_ms=500,
            max_records=10,
        )

        asyncio.run(consumer.exec())
