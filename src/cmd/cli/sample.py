import asyncio

from asynch import Connection, Pool
from clickhouse_driver import Client, connect
from clickhouse_driver.dbapi.extras import DictCursor
from loguru import logger

from src.pkg.driver.clickhouse._main import ClickhouseDriver

from ._base import BaseCliCmd


async def connect_database():
    async with Connection(
        user="root",
        password="password",
        host="0.0.0.0",
        port=9000,
        database="default",
    ) as conn:
        async with conn.cursor() as curr:
            await curr.execute("SELECT 1;")
            res = await curr.fetchall()

        print("RES = ", res)


def test_driver():
    # client = Client.from_url(url="clickhouse://root:password@0.0.0.0:8123/default")
    client = Client.from_url(url="clickhouse://root:password@0.0.0.0:9000/default")

    res = client.execute("select 1;")
    print("RES = ", res)

    with connect("clickhouse://root:password@0.0.0.0:9000/default") as conn:
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("select 1;")
            print(cursor.fetchone())


async def use_pool():
    # init a Pool and fill it with the `minsize` opened connections
    async with Pool(
        minsize=1, maxsize=2, dsn="clickhouse://root:password@0.0.0.0:9000/default"
    ) as pool:
        # acquire a connection from the pool
        async with pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                ret = await cursor.fetchone()
                print("RET = ", ret)


async def test_clikchouse():
    driver = ClickhouseDriver(
        host="0.0.0.0",
        port="9000",
        username="root",
        password="password",
        db="default",
    )

    result = await driver.force_select("select 1;")
    print("result = ", result)

    result = await driver.force_select("select 1;")
    print("result = ", result)

    result = await driver.force_select("select 1;")
    print("result = ", result)

    print("free_connections = ", driver.pool.free_connections)
    print("acquired_connections = ", driver.pool.acquired_connections)
    await asyncio.sleep(10)


class CreateSampleCmd(BaseCliCmd):
    name = "CreateSampleCli"

    def run(self) -> None:
        with logger.contextualize(request_id=""):
            self._prepare()
            # controller = CreateSampleController()
            # asyncio.run(controller.run())
            # asyncio.run(test_clikchouse())
            # asyncio.run(connect_database())
            test_driver()
            # asyncio.run(use_pool())
