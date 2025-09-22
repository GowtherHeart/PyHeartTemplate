from src.pkg.driver._base import UQuery

from ._main import ClickhouseDriver


class InvalidClickhouseQueryError(Exception): ...


class ClickhouseSpecQuery(UQuery):
    table: str
    columns: list[str]
    db: str | None = None
    driver: ClickhouseDriver

    def __init__(self, param) -> None:
        self.param = param


class InsertQuery(ClickhouseSpecQuery):
    async def execute(self) -> dict:
        if not self.table or not self.columns:
            raise InvalidClickhouseQueryError()

        res = await self.driver.insert(
            table=self.table,
            data=self.param,
            column_names=self.columns,
        )
        return res
