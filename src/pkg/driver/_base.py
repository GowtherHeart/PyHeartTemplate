from typing import Protocol


class DriverProtocol(Protocol):

    name: str

    async def force_select(self, query: str, *args, **kwargs): ...

    async def transaction_select(self, query: str, *args, **kwargs): ...


class UQuery: ...
