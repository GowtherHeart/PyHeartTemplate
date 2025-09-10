from src.pkg.abc.controller import ConsumerController


class SampleController(ConsumerController):

    model = str

    async def execute(self, payload):
        print(payload)
