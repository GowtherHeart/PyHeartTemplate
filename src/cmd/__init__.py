from src.cmd._test import TestCmd
from src.cmd.cli.send_msg import SendMsgCmd
from src.cmd.consumer._main import CoreConsumerCmd
from src.cmd.http import HttpCmd
from src.pkg.abc.cmd import Mapper as _Mapper


class Mapper(_Mapper):
    MAP = {
        HttpCmd.name: HttpCmd,
        CoreConsumerCmd.name: CoreConsumerCmd,
        SendMsgCmd.name: SendMsgCmd,
        TestCmd.name: TestCmd,
    }
