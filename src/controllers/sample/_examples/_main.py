from src.models.response import sample as notes_resp

SampleCoreResponseModelExample = notes_resp.SampleCoreRespModel(
    name="name",
    content="content",
    date_update="2025-01-01",  # type: ignore
    date_create="2025-01-01",  # type: ignore
)

SampleCoreResponseModelArrayExample = [SampleCoreResponseModelExample]
