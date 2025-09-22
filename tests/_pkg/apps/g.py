from ._base import App


class GlobalTestApp(App):
    def __init__(self) -> None:
        from src import Mapper
        from src.config.app import Config

        cmd_obj = Mapper().MAP["_Test"]
        Config(cmd_obj.config_array)  # type: ignore
        app = Mapper().MAP["_Test"]()
        app._prepare()  # type: ignore
        self._app = app

    @property
    def app(self):
        return self._app
