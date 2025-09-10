from src.cmd.cli.sample import CreateSampleCmd
from src.cmd.consumer._main import CoreConsumerCmd
from src.cmd.http import HttpCmd
from src.pkg.abc.cmd import Mapper as _Mapper


class Mapper(_Mapper):
    MAP = {
        HttpCmd.name: HttpCmd,
        CreateSampleCmd.name: CreateSampleCmd,
        CoreConsumerCmd.name: CoreConsumerCmd,
    }
