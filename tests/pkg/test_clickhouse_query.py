import pytest

from src.pkg.driver.clickhouse.query import InsertQuery, InvalidClickhouseQueryError


class FakeDriver:
    async def insert(self, *, table, data, column_names):
        # Echo back a summary-like structure
        return {"rows": len(data), "table": table, "columns": column_names}


@pytest.mark.asyncio
async def test_clickhouse_insert_query_validation_and_call():
    # Missing table/columns should raise
    q = InsertQuery(param=[[1, 2]])
    q.table = ""
    q.columns = []
    q.driver = FakeDriver()  # type: ignore[attr-defined]
    with pytest.raises(InvalidClickhouseQueryError):
        await q.execute()

    # Proper configuration should call driver.insert and return its summary
    q.table = "events"
    q.columns = ["a", "b"]
    res = await q.execute()
    assert res == {"rows": 1, "table": "events", "columns": ["a", "b"]}
