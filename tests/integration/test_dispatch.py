"""The dispatcher: publishes credits the API couldn't, without feeding a backlog."""

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pymongo.database import Database

from credit_service.db import Document
from credit_service.models import CreditStatus
from credit_service.repository import CreditRepository
from credit_service.services import CreditService


def broker_down(_: str) -> None:
    raise ConnectionError("broker down")


def make_due(db: Database[Document], *task_ids: str) -> None:
    db.credit_operations.update_many(
        {"_id": {"$in": list(task_ids)}},
        {"$set": {"next_dispatch_at": datetime.now(UTC) - timedelta(seconds=1)}},
    )


def insert_unpublished(db: Database[Document], count: int) -> list[str]:
    """Credits accepted while the broker was down, inserted in bulk."""
    past = datetime.now(UTC) - timedelta(minutes=5)
    docs = [
        {
            "_id": f"cr_bulk_{i:06d}",
            "client_id": "anonymous",
            "user_id": "u_123",
            "amount": Decimal("1.00"),
            "status": CreditStatus.PROCESSING,
            "created_at": past,
            "updated_at": past,
            "published_at": None,
            "dispatch_attempts": 0,
            "next_dispatch_at": past + timedelta(milliseconds=i),
        }
        for i in range(count)
    ]
    db.credit_operations.insert_many(docs)
    return [doc["_id"] for doc in docs]


def test_unpublished_credit_is_dispatched_after_grace_period(
    make_service: Callable[..., CreditService],
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    make_user("u_123")
    published: list[str] = []
    operation = make_service(broker_down).submit_credit("u_123", Decimal("10"))
    dispatcher = make_service(published.append)

    assert dispatcher.dispatch_pending().published == 0  # API's own publish gets a head start

    make_due(db, operation.task_id)
    assert dispatcher.dispatch_pending().published == 1
    assert published == [operation.task_id]
    stored = db.credit_operations.find_one({"_id": operation.task_id})
    assert stored is not None
    assert stored["published_at"] is not None

    make_due(db, operation.task_id)
    assert dispatcher.dispatch_pending().published == 0  # confirmed now; queue depth unknown


@pytest.mark.parametrize(("depth", "republished"), [(None, False), (5_000, False), (0, True)])
def test_confirmed_credits_are_only_republished_while_queue_is_drained(
    make_service: Callable[..., CreditService],
    service: CreditService,
    db: Database[Document],
    make_user: Callable[..., str],
    depth: int | None,
    republished: bool,
) -> None:
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("10"))  # published and confirmed
    make_due(db, operation.task_id)  # ...and unprocessed for longer than republish_after
    published: list[str] = []

    report = make_service(published.append, queue_depth=lambda: depth).dispatch_pending()

    assert report.reconciled is republished
    assert published == ([operation.task_id] if republished else [])


def test_broker_failure_mid_batch_keeps_progress_and_releases_the_rest(
    make_service: Callable[..., CreditService], db: Database[Document]
) -> None:
    task_ids = insert_unpublished(db, 10)
    published: list[str] = []

    def flaky(task_id: str) -> None:
        if len(published) == 3:
            raise ConnectionError("broker down")
        published.append(task_id)

    report = make_service(flaky).dispatch_pending()
    assert (report.published, report.broker_available) == (3, False)

    # The unpublished seven are due again immediately, not after the lease.
    rest = make_service(published.append).dispatch_pending()
    assert rest.published == 7
    assert sorted(published) == sorted(task_ids)


def test_drains_a_large_backlog_in_one_run_without_duplicates(
    make_service: Callable[..., CreditService], db: Database[Document]
) -> None:
    task_ids = insert_unpublished(db, 10_000)
    published: list[str] = []

    started = time.monotonic()
    report = make_service(published.append).dispatch_pending()

    assert report.published == 10_000
    assert sorted(published) == sorted(task_ids)
    assert db.credit_operations.count_documents({"published_at": None}) == 0
    assert time.monotonic() - started < 10


def test_concurrent_dispatchers_never_publish_the_same_credit_twice(
    make_service: Callable[..., CreditService], db: Database[Document]
) -> None:
    task_ids = insert_unpublished(db, 2_000)
    published: list[str] = []

    with ThreadPoolExecutor(max_workers=4) as pool:
        reports = list(
            pool.map(
                lambda _: make_service(published.append, dispatch_batch_size=50).dispatch_pending(),
                range(4),
            )
        )

    assert sum(r.published for r in reports) == 2_000
    assert sorted(published) == sorted(task_ids)


def test_dispatcher_crash_after_publish_only_causes_a_harmless_duplicate(
    make_service: Callable[..., CreditService],
    service: CreditService,
    db: Database[Document],
    make_user: Callable[..., str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    make_user("u_123", balance=Decimal("100.00"))
    operation = make_service(broker_down).submit_credit("u_123", Decimal("10"))
    make_due(db, operation.task_id)
    published: list[str] = []

    def crash(self: CreditRepository, task_ids: list[str]) -> None:
        raise RuntimeError("dispatcher died before recording the publish")

    with monkeypatch.context() as patch:
        patch.setattr(CreditRepository, "mark_published", crash)
        with pytest.raises(RuntimeError):
            make_service(published.append).dispatch_pending()

    make_due(db, operation.task_id)  # the claim's lease expires
    make_service(published.append).dispatch_pending()
    assert published == [operation.task_id, operation.task_id]

    for task_id in published:  # both messages get processed
        service.apply_credit(task_id)
    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert user["balance"] == Decimal("110.00")


def test_settled_credits_are_never_dispatched(
    make_service: Callable[..., CreditService],
    service: CreditService,
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("10"))
    service.apply_credit(operation.task_id)
    make_due(db, operation.task_id)
    published: list[str] = []

    assert make_service(published.append, queue_depth=lambda: 0).dispatch_pending().published == 0
    assert published == []
