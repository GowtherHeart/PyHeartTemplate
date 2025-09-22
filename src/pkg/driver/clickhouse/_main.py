import hashlib
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient


class _Singleton(type):
    """A metaclass for creating singleton classes with parameter-based instantiation.

    Unlike the traditional singleton pattern, this implementation allows for multiple
    instances of a class, each associated with a unique set of initialization parameters.
    The uniqueness of each instance is determined by hashing the provided arguments and
    keyword arguments. If an instance with the same parameter hash already exists, it is
    returned; otherwise, a new instance is created and stored.

    Attributes:
        _inst_map (dict): A dictionary mapping parameter hashes to their corresponding instances.
    """

    _inst_map: dict = {}

    def _merge_param(cls, *args, **kwargs) -> str:
        result: list[Any] = []
        result.extend(str(v) for v in args)
        result.extend(str(v) for v in kwargs.values())

        m = hashlib.sha256()
        m.update("".join(result).encode("utf-8"))
        return m.hexdigest()

    def __call__(cls, *args, **kwargs):
        param_hex = cls._merge_param(*args, **kwargs)
        if param_hex not in cls._inst_map:
            cls._inst_map[param_hex] = super().__call__(*args, **kwargs)

        return cls._inst_map[param_hex]


class ClickhouseDriver(metaclass=_Singleton):
    name = "clickhouse"

    pool = None  # type: ignore
    client: AsyncClient = None  # type: ignore

    def __init__(
        self, host: str, port: str, username: str, password: str, db: str
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self.db = db

    async def init_client(self):
        if self.client is None:
            self.client = await clickhouse_connect.get_async_client(
                host=self._host,
                port=int(self._port),
                username=self._username,
                password=self._password,
                database=self.db,
            )

    async def force_select(self, query: str, *args) -> Any:
        await self.init_client()
        res = await self.client.query(query)
        res = [i for i in res.named_results()]  # type: ignore
        return res

    async def transaction_select(self, query: str, *args) -> Any:
        await self.init_client()
        res = await self.client.query(query)
        res = [i for i in res.named_results()]  # type: ignore
        return res

    async def insert(self, table: str, data: list[list], column_names: list) -> dict:
        await self.init_client()
        res = await self.client.insert(
            table=table,
            data=data,
            column_names=column_names,
        )
        return res.summary
