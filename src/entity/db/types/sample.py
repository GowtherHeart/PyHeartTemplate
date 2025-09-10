from datetime import datetime
from typing import TypeAlias


class SampleTyping:
    """Type annotations for Note entity fields."""

    id: TypeAlias = int
    name: TypeAlias = str
    content: TypeAlias = str | None
    date_create: TypeAlias = datetime
    date_update: TypeAlias = datetime
    deleted: TypeAlias = bool


class SampleCustomTyping:
    """Custom type annotations for optional Note entity fields."""

    name: TypeAlias = SampleTyping.name | None
    date_create: TypeAlias = SampleTyping.date_create | None
    deleted: TypeAlias = SampleTyping.deleted | None
