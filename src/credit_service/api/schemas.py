from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PlainSerializer, WithJsonSchema

from credit_service.models import CreditStatus
from credit_service.money import format_amount, parse_amount

Amount = Annotated[
    Decimal,
    BeforeValidator(parse_amount),
    PlainSerializer(format_amount, return_type=str),
    WithJsonSchema({"type": "string", "pattern": r"^\d+(\.\d{1,2})?$", "examples": ["50.00"]}),
]

UserId = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")]


class CreditRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: UserId = Field(examples=["u_123"])
    amount: Amount


class CreditAccepted(BaseModel):
    task_id: str
    status: CreditStatus
    status_url: str
