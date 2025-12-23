"""CRUD queue payload model."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from anystore.model import BaseModel
from pydantic import Field


class CrudResource(str, Enum):
    """Target resource"""

    ARCHIVE = "archive"
    STATEMENTS = "statements"
    ENTITIES = "entities"


class CrudAction(str, Enum):
    """Action type for CRUD operations."""

    UPSERT = "upsert"
    DELETE = "delete"


class Crud(BaseModel):
    """
    Payload model for CRUD queue operations.

    All lakehouse mutations go through this single queue, ordered by UUID7.
    The queue key (UUID7) is managed by anystore.Queue, not stored in the model.
    """

    action: CrudAction
    resource: CrudResource
    payload: Any = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
