import asyncio
import os
from uuid import uuid4

import pytest

os.environ["TESTING"] = "true"


@pytest.yield_fixture(scope="session")
def event_loop(request):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


from src.pkg.context import make_tx_id
from tests._pkg.apps import GlobalTestApp, HttpApp

make_tx_id()

#################################
### INIT APPS
#################################
HttpApp()
GlobalTestApp()


@pytest.fixture
async def sample():
    from src.repository import sample as sample_repo

    return await sample_repo.CreateQuery(
        name=uuid4().hex, content=uuid4().hex
    ).execute()
