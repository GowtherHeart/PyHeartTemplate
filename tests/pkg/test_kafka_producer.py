from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_producer_send_monkeypatched(monkeypatch):
    # Import here to ensure pytest collection is stable even if aiokafka isn't available.
    from src.pkg.kafka.producer import ProducerKafka

    calls = SimpleNamespace(start=False, stop=False, sent=None)

    class FakeAIOKafkaProducer:
        def __init__(self, *args, **kwargs):
            self.kwargs = kwargs

        async def start(self):
            calls.start = True

        async def send_and_wait(self, topic, value):
            calls.sent = (topic, value)

        async def stop(self):
            calls.stop = True

    # Monkeypatch the aiokafka producer used in our wrapper
    monkeypatch.setattr("src.pkg.kafka.producer.AIOKafkaProducer", FakeAIOKafkaProducer)

    producer = ProducerKafka(
        bootstrap_server_array="kafka:9092",
        topic="test-topic",
        group_id="g1",
        sasl_username="user",
        sasl_password="pass",
    )

    await producer.send("hello")

    assert calls.start is True
    assert calls.stop is True
    assert calls.sent == ("test-topic", b"hello")
