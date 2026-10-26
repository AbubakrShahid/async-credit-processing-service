"""Celery delivers at least once, so the same credit task can run more than once.

These tests pin the guarantee that matters: the balance changes at most once per credit.
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from pymongo.database import Database

from credit_service.db import Document
from credit_service.models import ApplyOutcome
from credit_service.services import CreditService


def balance_of(db: Database[Document], user_id: str) -> Decimal:
    user = db.users.find_one({"user_id": user_id})
    assert user is not None
    balance: Decimal = user["balance"]
    return balance


def test_redelivered_task_does_not_credit_twice(
    service: CreditService, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123", balance=Decimal("100.00"))
    operation = service.submit_credit("u_123", Decimal("25.00"))

    # Second run = the worker committed, crashed before acking, and RabbitMQ redelivered.
    assert service.apply_credit(operation.task_id) is ApplyOutcome.APPLIED
    assert service.apply_credit(operation.task_id) is ApplyOutcome.ALREADY_SETTLED

    assert balance_of(db, "u_123") == Decimal("125.00")
    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert user["pending_credits"] == 0


def test_concurrent_deliveries_of_same_task_credit_once(
    service: CreditService, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("10.00"))

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(lambda _: service.apply_credit(operation.task_id), range(8)))

    assert outcomes.count(ApplyOutcome.APPLIED) == 1
    assert outcomes.count(ApplyOutcome.ALREADY_SETTLED) == 7
    assert balance_of(db, "u_123") == Decimal("10.00")
