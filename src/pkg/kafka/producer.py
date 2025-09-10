from aiokafka import AIOKafkaProducer

from ._base import _Singleton


class ProducerKafka(metaclass=_Singleton):
    def __init__(
        self,
        host: str,
        port: str,
        topic: str,
        group_id: str,
        sasl_username: str,
        sasl_password: str,
    ) -> None:
        self.host = host
        self.port = port
        self.topic = topic
        self.group_id = group_id
        self.__sasl_username = sasl_username
        self.__sasl_password = sasl_password

    async def send(self, msg: str) -> None:
        producer = AIOKafkaProducer(
            bootstrap_servers="{}:{}".format(self.host, self.port),
            sasl_plain_username=self.__sasl_username,
            sasl_plain_password=self.__sasl_password,
        )
        await producer.start()
        try:
            await producer.send_and_wait(self.topic, msg.encode("utf-8"))
        finally:
            await producer.stop()
