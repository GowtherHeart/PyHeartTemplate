from src.internal.exception import EmptyResultException
from src.internal.transaction import transaction
from src.models.db.sample import SampleCoreModel
from src.models.request import sample as sample_req
from src.pkg.abc.usecase import Usecase
from src.repository import sample as sample_repo


class SampleV1US(Usecase):
    """Use case class for sample business logic operations (version 1)."""

    async def get(self, model: sample_req.GetPrmModel) -> list[SampleCoreModel]:
        """Retrieve sample objs with optional filtering and pagination."""
        return await sample_repo.SelectQuery(
            name=model.name,
            date_create=model.date_create,
            limit=model.limit,
            offset=model.offset,
        ).execute()

    @transaction
    async def create(self, payload: sample_req.CreatePldModel) -> SampleCoreModel:
        """Create a new note with transaction management.

        Args:
            payload (sample_req.CreatePldModel): Note creation data containing
                                             name and content

        Returns:
            SampleCoreModel: The created note with generated ID and timestamps

        Raises:
            NoteCreateException: If note creation fails due to validation or
                               database constraints

        Examples:
            note = await usecase.create(CreatePldModel(
                name="My New Note",
                content="This is the note content"
            ))
        """
        return await sample_repo.CreateQuery(
            name=payload.name,
            content=payload.content,
        ).execute()

    @transaction
    async def update(self, payload: sample_req.UpdatePldModel) -> SampleCoreModel:
        """Update an existing note with transaction management.

        Args:
            payload (sample_req.UpdatePldModel): Note update data containing
                                             name and content

        Returns:
            SampleCoreModel: The updated note with new values and updated timestamp

        Raises:
            EmptyResultException: If no note was found to update
            NoteUpdateException: If note update fails due to validation or
                               database constraints

        Examples:
            updated_note = await usecase.update(UpdatePldModel(
                name="Updated Note Title",
                content="Updated note content"
            ))
        """
        effect = await sample_repo.UpdateQuery(
            name=payload.name,
            content=payload.content,
        ).execute()
        if len(effect) == 0:
            raise EmptyResultException

        return effect[0]
