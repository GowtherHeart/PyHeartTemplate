from pydantic import Field

from src.pkg.abc.entity import Entity, FieldEntity

from .types.sample import SampleCustomTyping, SampleTyping


class SampleEntity(Entity):
    """Database entity representing Sample with all core fields."""

    class id(FieldEntity):
        id: SampleTyping.id = Field(...)

    class name(FieldEntity):
        name: SampleTyping.name = Field(...)

    class content(FieldEntity):
        content: SampleTyping.content = Field(...)

    class date_create(FieldEntity):
        date_create: SampleTyping.date_create = Field(...)

    class date_update(FieldEntity):
        date_update: SampleTyping.date_update = Field(...)


class SampleCustomEntity(Entity):
    """Custom database entity for optional note fields and operations."""

    class name_op(FieldEntity):
        """Optional note name field entity for custom operations."""

        name: SampleCustomTyping.name = Field(None)

    class date_create_op(FieldEntity):
        """Optional note creation date field entity for custom operations."""

        date_create: SampleCustomTyping.date_create = Field(None)
