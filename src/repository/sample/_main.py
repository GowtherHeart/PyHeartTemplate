from asyncpg import UniqueViolationError

from src.entity.db.types.core import CoreTyping
from src.entity.db.types.sample import SampleCustomTyping, SampleTyping
from src.internal.exception.sample import SampleCreateException, SampleUpdateException
from src.models.db.sample import SampleCoreModel
from src.pkg.driver.query import QueryExecute, QueryTxExecute

__all__ = ["CreateQuery", "SelectQuery", "UpdateQuery"]


class CreateQuery(QueryTxExecute):
    query = """
        insert into sample(name, content)
        values($1, $2)
        returning id, name, content, date_create, date_update;
    """

    exception_map = {UniqueViolationError: SampleCreateException}

    def __init__(
        self,
        name: SampleTyping.name,
        content: SampleTyping.content,
    ) -> None:
        super().__init__(name, content)

    async def execute(self) -> SampleCoreModel:
        return await super().execute()


class UpdateQuery(QueryExecute):
    query = """
        update sample set
            content = COALESCE($1, content)
        where true
            and name = $2
        returning id, name, content, date_create, date_update;
    """
    exception_map = {UniqueViolationError: SampleUpdateException}

    def __init__(
        self, name: SampleCustomTyping.name = None, content: SampleTyping.content = None
    ) -> None:
        super().__init__(content, name)

    async def execute(self) -> list[SampleCoreModel]:
        return await super().execute()


class SelectQuery(QueryExecute):
    query = """
        select
            n.id as id,
            n.name as name,
            n.content as content,
            n.date_create as date_create,
            n.date_update as date_update
        from sample n
        where true
            and ($1::text is null or n.name = $1)
            and ($2::timestamp is null or n.date_create = $2)
        limit $3
        offset $4;
    """

    def __init__(
        self,
        name: SampleCustomTyping.name,
        date_create: SampleCustomTyping.date_create,
        limit: CoreTyping.limit = 100,
        offset: CoreTyping.offset = 0,
    ) -> None:
        super().__init__(name, date_create, limit, offset)

    async def execute(self) -> list[SampleCoreModel]:
        return await super().execute()
