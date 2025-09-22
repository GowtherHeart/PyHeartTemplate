import pytest

from src.models.db.sample import SampleCoreModel


@pytest.mark.asyncio
async def test_get(sample: SampleCoreModel):
    print(sample)


@pytest.mark.asyncio
async def test_create_notes(sample: SampleCoreModel):
    print(sample)
