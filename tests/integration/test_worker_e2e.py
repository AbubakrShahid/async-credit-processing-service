"""Full path: HTTP -> MongoDB -> RabbitMQ -> Celery worker -> MongoDB."""

from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pymongo.database import Database
from pymongo.errors import AutoReconnect, OperationFailure

from credit_service import worker as worker_module
from credit_service.api.app import app
from credit_service.celery_app import celery_app, enqueue_credit
from credit_service.config import Settings
from credit_service.db import Document
from credit_service.models import ApplyOutcome
from credit_service.repository import CreditRepository
from credit_service.services import CreditService
from tests.integration.helpers import running_worker, wait_for


@pytest.fixture
def worker() -> Iterator[None]:
    with running_worker():
        yield


def test_credit_is_applied_by_worker(
    worker: None, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123", balance=Decimal("100.00"))

    with TestClient(app) as client:
        response = client.post("/credits", json={"user_id": "u_123", "amount": "50.00"})
    assert response.status_code == 202
    task_id = response.json()["task_id"]

    def processed() -> bool:
        operation = db.credit_operations.find_one({"_id": task_id})
        return operation is not None and operation["status"] == "processed"

    wait_for(processed)
    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert (user["balance"], user["status"]) == (Decimal("150.00"), "processed")


def test_message_redelivered_after_commit_is_a_no_op(
    worker: None, service: CreditService, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123", balance=Decimal("100.00"))
    operation = service.submit_credit("u_123", Decimal("50.00"))
    service.apply_credit(operation.task_id)

    # The same message arrives again through RabbitMQ, as after a crash before the ack.
    enqueue_credit(operation.task_id)
    marker = service.submit_credit("u_123", Decimal("1.00"))
    enqueue_credit(marker.task_id)

    def marker_processed() -> bool:
        doc = db.credit_operations.find_one({"_id": marker.task_id})
        return doc is not None and doc["status"] == "processed"

    # Prefetch is 1 and the queue is FIFO, so once the marker is done the duplicate was too.
    wait_for(marker_processed)
    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert user["balance"] == Decimal("151.00")


def test_transient_mongo_error_is_retried(
    worker: None,
    service: CreditService,
    db: Database[Document],
    make_user: Callable[..., str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    make_user("u_123")
    operation = service.submit_credit("u_123", Decimal("20.00"))

    real_apply = CreditRepository.apply_credit
    calls = 0

    def flaky_apply(self: CreditRepository, task_id: str) -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise AutoReconnect("primary stepped down")
        return real_apply(self, task_id)

    monkeypatch.setattr(CreditRepository, "apply_credit", flaky_apply)
    monkeypatch.setattr(worker_module, "retry_delay", lambda *_: 0)
    enqueue_credit(operation.task_id)

    def processed() -> bool:
        doc = db.credit_operations.find_one({"_id": operation.task_id})
        return doc is not None and doc["status"] == "processed"

    wait_for(processed)
    doc = db.credit_operations.find_one({"_id": operation.task_id})
    assert doc is not None
    assert doc["attempts"] == 2


def test_credit_lost_at_publish_is_recovered_by_dispatcher(
    worker: None,
    make_service: Callable[..., CreditService],
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    make_user("u_123", balance=Decimal("100.00"))

    def broker_down(_: str) -> None:
        raise ConnectionError("broker down")

    operation = make_service(broker_down).submit_credit("u_123", Decimal("5.00"))
    db.credit_operations.update_one(
        {"_id": operation.task_id},
        {"$set": {"next_dispatch_at": datetime.now(UTC) - timedelta(seconds=1)}},
    )

    assert worker_module.dispatch_pending_credits() == 1

    def processed() -> bool:
        doc = db.credit_operations.find_one({"_id": operation.task_id})
        return doc is not None and doc["status"] == "processed"

    wait_for(processed)
    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert user["balance"] == Decimal("105.00")


def spy_on_apply(monkeypatch: pytest.MonkeyPatch) -> list[tuple[bool, object]]:
    """Record (redelivered, outcome-or-exception) for every apply the worker runs."""
    calls: list[tuple[bool, object]] = []
    real_apply = CreditService.apply_credit

    def spy(self: CreditService, task_id: str, *, redelivered: bool = False) -> ApplyOutcome:
        try:
            outcome = real_apply(self, task_id, redelivered=redelivered)
        except Exception as exc:
            calls.append((redelivered, type(exc)))
            raise
        calls.append((redelivered, outcome))
        return outcome

    monkeypatch.setattr(CreditService, "apply_credit", spy)
    return calls


def test_worker_crash_after_commit_before_ack_does_not_double_credit(
    service: CreditService,
    settings: Settings,
    db: Database[Document],
    make_user: Callable[..., str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    make_user("u_123", balance=Decimal("100.00"))
    operation = service.submit_credit("u_123", Decimal("50.00"))
    celery_app.control.purge()
    enqueue_credit(operation.task_id)

    # A consumer takes the real message, commits the credit, then dies before acking:
    # its connection drops with the message unacked, so RabbitMQ requeues it.
    connection = celery_app.connection_for_write()
    channel: Any = connection.channel()
    message = channel.basic_get(queue=settings.credits_queue, no_ack=False)
    assert message is not None
    service.apply_credit(operation.task_id)
    connection.close()

    calls = spy_on_apply(monkeypatch)
    with running_worker(purge=False):
        wait_for(lambda: len(calls) == 1)

    assert calls == [(True, ApplyOutcome.ALREADY_SETTLED)]  # redelivered, and a no-op
    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert (user["balance"], user["pending_credits"]) == (Decimal("150.00"), 0)


def test_lost_commit_reply_in_worker_does_not_double_credit(
    worker: None,
    service: CreditService,
    db: Database[Document],
    make_user: Callable[..., str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    make_user("u_123", balance=Decimal("100.00"))
    operation = service.submit_credit("u_123", Decimal("50.00"))

    # The commit succeeds but the reply is lost; MongoDB labels this
    # UnknownTransactionCommitResult and the worker cannot know what happened.
    real_transaction = CreditRepository._transaction
    lost: list[bool] = []

    def commit_reply_lost(self: CreditRepository, callback: Any) -> Any:
        result = real_transaction(self, callback)
        if not lost:
            lost.append(True)
            raise OperationFailure(
                "connection reset before commit reply",
                details={"errorLabels": ["UnknownTransactionCommitResult"]},
            )
        return result

    monkeypatch.setattr(CreditRepository, "_transaction", commit_reply_lost)
    monkeypatch.setattr(worker_module, "retry_delay", lambda *_: 0)
    calls = spy_on_apply(monkeypatch)
    enqueue_credit(operation.task_id)

    wait_for(lambda: len(calls) == 2)
    assert calls == [(False, OperationFailure), (False, ApplyOutcome.ALREADY_SETTLED)]
    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert (user["balance"], user["status"], user["pending_credits"]) == (
        Decimal("150.00"),
        "processed",
        0,
    )
