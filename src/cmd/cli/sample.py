import asyncio

from loguru import logger

from src.controllers.sample.cli import CreateSampleController

from ._base import BaseCliCmd


class CreateSampleCmd(BaseCliCmd):
    name = "CreateSampleCli"

    def run(self) -> None:
        with logger.contextualize(request_id=""):
            self._prepare()
            controller = CreateSampleController()
            asyncio.run(controller.run())
