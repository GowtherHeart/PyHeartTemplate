from src.cmd.cli.sample import CreateSampleCmd
from src.cmd.http import HttpCmd
from src.pkg.abc.cmd import Mapper as _Mapper


class Mapper(_Mapper):
    MAP = {HttpCmd.name: HttpCmd, CreateSampleCmd.name: CreateSampleCmd}
