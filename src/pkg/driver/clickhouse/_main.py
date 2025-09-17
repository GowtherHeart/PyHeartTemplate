import hashlib
from typing import Any

from asynch import Connection, DictCursor, Pool


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

    pool: Pool = None  # type: ignore

    def __init__(
        self, host: str, port: str, username: str, password: str, db: str
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self.db = db

    async def _init_pool(self) -> None:
        if self.pool is None:
            self.pool = Pool(
                minsize=1,
                maxsize=10,
                # maxsize=1000,
                dsn="clickhouse://{}:{}@{}:{}/{}".format(
                    self._username,
                    self._password,
                    self._host,
                    self._port,
                    self.db,
                ),
            )

    async def force_select(self, query: str, *args) -> Any:
        # await self._init_pool()
        # async with self.pool as pool:
        #     print("POOL = ", pool)
        #     async with pool.connection() as conn:
        #         print("CONN = ", conn)
        #         async with conn.cursor(cursor=DictCursor) as cursor:
        #             print("CURSOR = ", cursor)
        #             await cursor.execute("SELECT 1;")
        #             res = await cursor.fetchone()

        # print("=1=")
        async with Connection(
            user="root",
            password="password",
            host="0.0.0.0",
            port=9000,
            database="default",
        ) as conn:
            async with conn.cursor(cursor=DictCursor) as curr:
                await curr.execute("SELECT 1;")
                res = await curr.fetchone()
        #
        # print(res)

        # with connect('clickhouse://root:password@0.0.0.0:9000/default') as conn:
        #     with conn.cursor(cursor_factory=hueta_dict) as cursor:
        #        cursor.execute('select 1;')
        #        res = cursor.fetchone()

        return res

    async def transaction_select(self, query: str, *args) -> Any:
        await self._init_pool()
        async with self.pool as pool:
            print("POOL = ", pool)
            async with pool.connection() as conn:
                print("CONN = ", conn)
                async with conn.cursor(cursor=DictCursor) as cursor:
                    print("CURSOR = ", cursor)
                    await cursor.execute("SELECT 1;")
                    ret = await cursor.fetchone()

        return ret
