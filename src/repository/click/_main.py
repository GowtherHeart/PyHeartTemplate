from src.pkg.driver.query import QueryExecute


class SelectQueryClick(QueryExecute):
    query = """
        select 1;
    """

    def __init__(self) -> None:
        super().__init__()

    async def execute(self) -> dict:
        return await super().execute()
