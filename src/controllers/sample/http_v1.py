from starlette import status

from src.entity.db.types.core import CoreTyping
from src.entity.db.types.sample import SampleCustomTyping
from src.internal.exception import EmptyResultException, SampleCreateException
from src.internal.exception.sample import SampleUpdateException
from src.internal.fastapi.controller import HttpController
from src.models.request import sample as sample_req
from src.models.response.sample import SampleCoreRespModel
from src.pkg.abc.controller import router
from src.usecase.sample import SampleV1US

from ._examples import (
    SampleCoreResponseModelArrayExample,
    SampleCoreResponseModelExample,
)


class SampleCoreControllerV1(HttpController):
    """HTTP API controller for Sample management (version 1)."""

    prefix = "/sample"
    tags = ["core"]

    @router(
        path="/",
        status_code=status.HTTP_200_OK,
        response_model=SampleCoreResponseModelArrayExample,
    )
    async def get(
        self,
        name: SampleCustomTyping.name = None,
        date_create: SampleCustomTyping.date_create = None,
        limit: CoreTyping.limit = 100,
        offset: CoreTyping.offset = 0,
    ) -> list[SampleCoreRespModel]:
        """Retrieve a list of Sample with optional filtering and pagination."""
        model = sample_req.GetPrmModel(
            name=name,
            date_create=date_create,
            limit=limit,
            offset=offset,
        )
        result = await SampleV1US().get(model=model)
        return [SampleCoreRespModel(**e.model_dump()) for e in result]

    @router(
        path="/",
        status_code=status.HTTP_201_CREATED,
        responses={
            **SampleCreateException.generate_openapi(),
            **EmptyResultException.generate_openapi(),
        },
        response_model=SampleCoreResponseModelExample,
    )
    async def post(self, payload: sample_req.CreatePldModel) -> SampleCoreRespModel:
        """Create a new Sample with the provided data."""
        result = await SampleV1US().create(payload=payload)
        return SampleCoreRespModel(**result.model_dump())

    @router(
        path="/",
        status_code=status.HTTP_200_OK,
        responses={
            **SampleUpdateException.generate_openapi(),
            **EmptyResultException.generate_openapi(),
        },
        response_model=SampleCoreResponseModelExample,
    )
    async def patch(self, payload: sample_req.UpdatePldModel) -> SampleCoreRespModel:
        """Update an existing Sample by name."""
        result = await SampleV1US().update(payload=payload)
        return SampleCoreRespModel(**result.model_dump())
