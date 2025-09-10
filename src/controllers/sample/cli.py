from loguru import logger

from src.models.request.sample import CreatePldModel
from src.pkg.abc.controller import CliController
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
