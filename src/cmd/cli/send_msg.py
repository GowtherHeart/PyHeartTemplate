import asyncio

from loguru import logger

from src.controllers.sample.cli import SendMsgController

from ._base import BaseCliCmd


class SendMsgCmd(BaseCliCmd):
    name = "SendMsg"

    def run(self) -> None:
        with logger.contextualize(request_id=""):
            self._prepare()
            controller = SendMsgController()
            asyncio.run(controller.run())
