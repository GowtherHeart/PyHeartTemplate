from src.entity.db.sample import SampleEntity
from src.pkg.abc.model import ResponseModel

__all__ = ["SampleCoreRespModel"]


class SampleCoreRespModel(
    ResponseModel,
    SampleEntity.name,
    SampleEntity.content,
    SampleEntity.date_create,
    SampleEntity.date_update,
):
    """Response model for sample obj."""
