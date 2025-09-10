from src.entity.db.core import CoreEntity
from src.entity.db.sample import SampleCustomEntity, SampleEntity
from src.pkg.abc.model import ParamsModel, PayloadModel

__all__ = ["CreatePldModel", "GetPrmModel", "UpdatePldModel"]


class GetPrmModel(
    ParamsModel,
    SampleCustomEntity.name_op,
    SampleCustomEntity.date_create_op,
    CoreEntity.limit,
    CoreEntity.offset,
):
    """Parameters model for retrieving sample with filters."""


class DeletePrmModel(
    ParamsModel,
    SampleEntity.name,
):
    """Parameters model for deleting."""


class CreatePldModel(
    PayloadModel,
    SampleEntity.name,
    SampleEntity.content,
):
    """Payload model for creating a new sample obj."""


class UpdatePldModel(
    PayloadModel,
    SampleEntity.name,
    SampleEntity.content,
):
    """Payload model for updating an existing sample obj."""
