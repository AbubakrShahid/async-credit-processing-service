from datetime import datetime
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, PlainSerializer, WithJsonSchema

from credit_service.models import CreditOperation, CreditStatus, User
from credit_service.money import format_amount, parse_amount

# Money goes over the wire as a decimal string ("150.00") so no client parses it as a float.
Money = Annotated[
    Decimal,
    PlainSerializer(format_amount, return_type=str),
    WithJsonSchema({"type": "string", "examples": ["150.00"]}),
]

# Incoming credit amounts: strings or numbers, positive, at most two decimal places.
CreditAmount = Annotated[
    Decimal,
    BeforeValidator(parse_amount),
    PlainSerializer(format_amount, return_type=str),
    WithJsonSchema({"type": "string", "pattern": r"^\d+(\.\d{1,2})?$", "examples": ["50.00"]}),
]

UserId = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")]


class CreditRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: UserId = Field(examples=["u_123"])
    amount: CreditAmount


class CreditAccepted(BaseModel):
    task_id: str
    status: CreditStatus
    status_url: str


class CreditDetails(BaseModel):
    task_id: str
    user_id: str
    amount: Money
    status: CreditStatus
    attempts: int
    error: str | None
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_operation(cls, operation: CreditOperation) -> "CreditDetails":
        return cls(
            task_id=operation.task_id,
            user_id=operation.user_id,
            amount=operation.amount,
            status=operation.status,
            attempts=operation.attempts,
            error=operation.error,
            created_at=operation.created_at,
            processed_at=operation.processed_at,
        )


class UserBalance(BaseModel):
    user_id: str
    balance: Money
    status: CreditStatus | None
    pending_credits: int

    @classmethod
    def from_user(cls, user: User) -> "UserBalance":
        return cls(
            user_id=user.user_id,
            balance=user.balance,
            status=user.status,
            pending_credits=user.pending_credits,
        )
