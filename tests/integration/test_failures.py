"""Failure policy: which problems fail a credit, which never do, and terminal-state races."""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from pymongo.database import Database
from pymongo.errors import AutoReconnect

from credit_service.db import Document
from credit_service.models import ApplyOutcome
from credit_service.repository import CreditRepository
from credit_service.services import CreditService


def operation_of(db: Database[Document], task_id: str) -> Document:
    doc = db.credit_operations.find_one({"_id": task_id})
    assert doc is not None
    return doc


def user_of(db: Database[Document], user_id: str) -> Document:
    doc = db.users.find_one({"user_id": user_id})
    assert doc is not None
    return doc


def test_credit_fails_when_user_was_deleted(
    service: CreditService, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("10"))
    db.users.delete_one({"user_id": "u_123"})

    assert service.apply_credit(operation.task_id) is ApplyOutcome.FAILED

    stored = operation_of(db, operation.task_id)
    assert (stored["status"], stored["error"]) == ("failed", "user 'u_123' not found")


def test_transient_errors_never_fail_a_credit(
    service: CreditService,
    repository: CreditRepository,
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("10"))

    # A long MongoDB outage: many executions that each end in a transient error.
    for _ in range(20):
        repository.start_attempt(operation.task_id, redelivered=False)
        service.record_failed_attempt(operation.task_id, AutoReconnect("down"), transient=True)

    assert service.apply_credit(operation.task_id) is ApplyOutcome.APPLIED
    assert operation_of(db, operation.task_id)["attempts"] == 21


def test_concurrent_duplicate_is_not_mistaken_for_a_crash(
    make_service: Callable[..., CreditService],
    repository: CreditRepository,
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    service = make_service(max_crashes=1)
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("10"))

    # Another worker is still in flight with a fresh (not redelivered) copy.
    repository.start_attempt(operation.task_id, redelivered=False)

    assert service.apply_credit(operation.task_id) is ApplyOutcome.APPLIED
    assert operation_of(db, operation.task_id)["crashes"] == 0


def test_poison_message_fails_after_crash_budget_without_touching_balance(
    make_service: Callable[..., CreditService],
    repository: CreditRepository,
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    service = make_service(max_crashes=2)
    make_user("u_123", balance=Decimal("100.00"))
    operation = service.submit_credit("u_123", Decimal("10"))

    # Each execution dies mid-flight; RabbitMQ redelivers the unacked message.
    repository.start_attempt(operation.task_id, redelivered=False)
    repository.start_attempt(operation.task_id, redelivered=True)  # crash 1

    assert service.apply_credit(operation.task_id, redelivered=True) is ApplyOutcome.FAILED

    stored = operation_of(db, operation.task_id)
    assert stored["status"] == "failed"
    assert stored["error"] == "processing crashed 2 times; giving up"
    user = user_of(db, "u_123")
    assert (user["balance"], user["status"], user["pending_credits"]) == (
        Decimal("100.00"),
        "failed",
        0,
    )


def test_unexpected_errors_count_towards_the_crash_budget(
    make_service: Callable[..., CreditService],
    repository: CreditRepository,
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    service = make_service(max_crashes=2)
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("10"))

    for _ in range(2):
        repository.start_attempt(operation.task_id, redelivered=False)
        service.record_failed_attempt(operation.task_id, KeyError("bug"), transient=False)

    assert service.apply_credit(operation.task_id) is ApplyOutcome.FAILED


def test_failed_credit_is_final(
    service: CreditService, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("10"))
    db.users.delete_one({"user_id": "u_123"})
    service.apply_credit(operation.task_id)
    make_user("u_123")

    assert service.apply_credit(operation.task_id) is ApplyOutcome.ALREADY_SETTLED


def test_racing_apply_and_fail_reach_exactly_one_terminal_state(
    service: CreditService,
    repository: CreditRepository,
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    make_user("u_123", balance=Decimal("0.00"))
    operations = [service.submit_credit("u_123", Decimal("1.00")) for _ in range(20)]

    def race(task_id: str) -> None:
        with ThreadPoolExecutor(max_workers=4) as pool:
            pool.submit(service.apply_credit, task_id)
            pool.submit(repository.fail_credit, task_id, "gave up")
            pool.submit(service.apply_credit, task_id)
            pool.submit(repository.fail_credit, task_id, "gave up")

    for operation in operations:
        race(operation.task_id)

    statuses = [operation_of(db, op.task_id)["status"] for op in operations]
    assert set(statuses) <= {"processed", "failed"}
    user = user_of(db, "u_123")
    # Whatever won each race, balance and pending count agree with the terminal states.
    assert user["balance"] == Decimal(statuses.count("processed"))
    assert user["pending_credits"] == 0
