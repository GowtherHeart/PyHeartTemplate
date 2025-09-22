from typing import List

from src.models.request.sample import CreatePldModel
from src.pkg.abc.controller import ConsumerController
from src.repository import sample as sample_repo


class SampleController(ConsumerController):

    model = CreatePldModel

    async def execute(self, payload: List[CreatePldModel], payload_err):
        # print("Payload = ", payload)
        # print("Payload_err = ", payload_err)
        for p in payload:
            await sample_repo.CreateQuery(name=p.name, content=p.content).execute()
