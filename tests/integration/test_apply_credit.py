from collections.abc import Callable
from decimal import Decimal

from pymongo.database import Database

from credit_service.db import Document
from credit_service.models import ApplyOutcome
from credit_service.services import CreditService


def test_creates_balance_field_when_missing(
    service: CreditService, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("50.00"))

    assert service.apply_credit(operation.task_id) is ApplyOutcome.APPLIED

    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert user["balance"] == Decimal("50.00")
    assert user["status"] == "processed"
    assert user["pending_credits"] == 0

    stored = db.credit_operations.find_one({"_id": operation.task_id})
    assert stored is not None
    assert stored["status"] == "processed"
    assert stored["processed_at"] is not None


def test_adds_to_existing_balance_without_float_drift(
    service: CreditService, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123", balance=Decimal("100.10"))
    operation = service.submit_credit("u_123", Decimal("0.20"))

    service.apply_credit(operation.task_id)

    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert user["balance"] == Decimal("100.30")


def test_user_stays_processing_while_other_credits_are_pending(
    make_service: Callable[..., CreditService],
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    service = make_service(apply_batch_size=1)  # settle one credit at a time
    make_user("u_123")
    first = service.submit_credit("u_123", Decimal("10"))
    second = service.submit_credit("u_123", Decimal("5"))

    service.apply_credit(first.task_id)
    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert (user["status"], user["pending_credits"]) == ("processing", 1)

    service.apply_credit(second.task_id)
    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert (user["status"], user["pending_credits"]) == ("processed", 0)
    assert user["balance"] == Decimal("15.00")


def test_unknown_task_is_dropped(service: CreditService) -> None:
    assert service.apply_credit("cr_does_not_exist") is ApplyOutcome.NOT_FOUND
