from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from pymongo.database import Database

from credit_service.db import Document
from credit_service.repository import CreditRepository
from credit_service.services import CreditService

STALE_AFTER = timedelta(minutes=2)


def broker_down(_: str) -> None:
    raise ConnectionError("broker down")


def age(db: Database[Document], task_id: str, by: timedelta = STALE_AFTER * 2) -> None:
    db.credit_operations.update_one(
        {"_id": task_id}, {"$set": {"last_enqueued_at": datetime.now(UTC) - by}}
    )


def test_unpublished_credit_is_requeued_once_stale(
    repository: CreditRepository,
    service: CreditService,
    db: Database[Document],
    make_user: Callable[..., str],
    published: list[str],
) -> None:
    make_user("u_123")
    operation = CreditService(repository, broker_down).submit_credit("u_123", Decimal("10"))

    assert service.requeue_stale_credits(STALE_AFTER, limit=100) == 0  # not stale yet

    age(db, operation.task_id)
    assert service.requeue_stale_credits(STALE_AFTER, limit=100) == 1
    assert published == [operation.task_id]

    # The claim refreshed last_enqueued_at, so an immediate second sweep skips it.
    assert service.requeue_stale_credits(STALE_AFTER, limit=100) == 0


def test_settled_credits_are_never_requeued(
    service: CreditService,
    db: Database[Document],
    make_user: Callable[..., str],
    published: list[str],
) -> None:
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("10"))
    service.apply_credit(operation.task_id)
    age(db, operation.task_id)
    published.clear()

    assert service.requeue_stale_credits(STALE_AFTER, limit=100) == 0
    assert published == []


def test_sweep_is_batched(
    service: CreditService,
    db: Database[Document],
    make_user: Callable[..., str],
    published: list[str],
) -> None:
    make_user("u_123")
    for _ in range(5):
        age(db, service.submit_credit("u_123", Decimal("1")).task_id)
    published.clear()

    assert service.requeue_stale_credits(STALE_AFTER, limit=3) == 3
    assert service.requeue_stale_credits(STALE_AFTER, limit=3) == 2


def test_failed_requeue_is_retried_on_a_later_sweep(
    repository: CreditRepository,
    service: CreditService,
    db: Database[Document],
    make_user: Callable[..., str],
    published: list[str],
) -> None:
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("10"))
    age(db, operation.task_id)

    assert CreditService(repository, broker_down).requeue_stale_credits(STALE_AFTER, 100) == 0

    age(db, operation.task_id)
    published.clear()
    assert service.requeue_stale_credits(STALE_AFTER, limit=100) == 1
    assert published == [operation.task_id]
