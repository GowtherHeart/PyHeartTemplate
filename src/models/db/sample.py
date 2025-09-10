from src.entity.db.sample import SampleEntity
from src.pkg.abc.model import DbModel

__all__ = ["SampleCoreModel"]


class SampleCoreModel(
    DbModel,
    SampleEntity.id,
    SampleEntity.name,
    SampleEntity.content,
    SampleEntity.date_create,
    SampleEntity.date_update,
):
    """Core database sample model."""
