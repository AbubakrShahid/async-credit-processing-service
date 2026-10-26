from collections.abc import Callable
from decimal import Decimal

from pymongo.database import Database

from credit_service.db import Document
from credit_service.models import ApplyOutcome
from credit_service.repository import CreditRepository
from credit_service.services import CreditService


def test_credit_fails_when_user_was_deleted(
    service: CreditService, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("10"))
    db.users.delete_one({"user_id": "u_123"})

    assert service.apply_credit(operation.task_id) is ApplyOutcome.FAILED

    stored = db.credit_operations.find_one({"_id": operation.task_id})
    assert stored is not None
    assert (stored["status"], stored["error"]) == ("failed", "user 'u_123' not found")


def test_credit_fails_after_max_attempts_without_touching_balance(
    repository: CreditRepository, db: Database[Document], make_user: Callable[..., str]
) -> None:
    service = CreditService(repository, lambda _: None, max_attempts=3)
    make_user("u_123", balance=Decimal("100.00"))
    operation = service.submit_credit("u_123", Decimal("10"))
    db.credit_operations.update_one({"_id": operation.task_id}, {"$set": {"attempts": 3}})

    assert service.apply_credit(operation.task_id) is ApplyOutcome.FAILED

    stored = db.credit_operations.find_one({"_id": operation.task_id})
    assert stored is not None
    assert stored["status"] == "failed"
    assert stored["error"] == "gave up after 3 attempts"
    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert (user["balance"], user["status"], user["pending_credits"]) == (
        Decimal("100.00"),
        "failed",
        0,
    )


def test_failed_credit_is_final(
    service: CreditService, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("10"))
    db.users.delete_one({"user_id": "u_123"})
    service.apply_credit(operation.task_id)
    make_user("u_123")

    assert service.apply_credit(operation.task_id) is ApplyOutcome.ALREADY_SETTLED
