import pytest


@pytest.mark.asyncio
async def test_sample_controller_model_and_execute(monkeypatch):
    from src.controllers.sample import kafka as kafka_module
    from src.controllers.sample.kafka import SampleController
    from src.models.request.sample import CreatePldModel

    calls: list[tuple[str, str | None]] = []

    class FakeCreateQuery:
        def __init__(self, *, name: str, content: str | None):
            # capture parameters passed in
            calls.append((name, content))

        async def execute(self):  # noqa: D401
            # do nothing, just emulate async DB call
            return {"ok": True}

    # Monkeypatch repository CreateQuery used inside controller
    monkeypatch.setattr(kafka_module.sample_repo, "CreateQuery", FakeCreateQuery)

    ctrl = SampleController()

    # Model should be the request model for creation
    assert ctrl.model is CreatePldModel

    payload = [
        CreatePldModel(name="n1", content="c1"),
        CreatePldModel(name="n2", content=None),
    ]

    await ctrl.execute(payload=payload, payload_err=[])

    # Repository called once per payload item with matching args
    assert calls == [("n1", "c1"), ("n2", None)]
