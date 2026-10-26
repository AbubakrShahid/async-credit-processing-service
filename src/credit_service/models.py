from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Self

from credit_service.db import Document


class CreditStatus(StrEnum):
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"


class ApplyOutcome(StrEnum):
    APPLIED = "applied"
    ALREADY_SETTLED = "already_settled"
    NOT_FOUND = "not_found"


@dataclass(frozen=True, slots=True)
class CreditOperation:
    task_id: str
    user_id: str
    amount: Decimal
    status: CreditStatus
    attempts: int
    error: str | None
    created_at: datetime
    updated_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_document(cls, doc: Document) -> Self:
        return cls(
            task_id=doc["_id"],
            user_id=doc["user_id"],
            amount=doc["amount"],
            status=CreditStatus(doc["status"]),
            attempts=doc.get("attempts", 0),
            error=doc.get("error"),
            created_at=doc["created_at"],
            updated_at=doc["updated_at"],
            processed_at=doc.get("processed_at"),
        )


@dataclass(frozen=True, slots=True)
class User:
    user_id: str
    balance: Decimal
    status: CreditStatus | None
    pending_credits: int

    @classmethod
    def from_document(cls, doc: Document) -> Self:
        status = doc.get("status")
        return cls(
            user_id=doc["user_id"],
            balance=doc.get("balance", Decimal("0.00")),
            status=CreditStatus(status) if status else None,
            pending_credits=doc.get("pending_credits", 0),
        )
