"""Full path: HTTP -> MongoDB -> RabbitMQ -> Celery worker -> MongoDB."""

import time
from collections.abc import Callable, Iterator
from decimal import Decimal

import pytest
from celery.contrib.testing.worker import start_worker
from fastapi.testclient import TestClient
from pymongo.database import Database
from pymongo.errors import AutoReconnect

from credit_service import worker as worker_module
from credit_service.api.app import app
from credit_service.celery_app import enqueue_credit
from credit_service.db import Document
from credit_service.repository import CreditRepository
from credit_service.services import CreditService
from credit_service.worker import celery_app


@pytest.fixture
def worker() -> Iterator[None]:
    celery_app.control.purge()
    # The worker's event loop only checks for shutdown when a timer fires; a short AMQP
    # heartbeat makes it stop in well under a second instead of ~10s.
    celery_app.conf.broker_heartbeat = 1
    with start_worker(celery_app, pool="solo", perform_ping_check=False, loglevel="WARNING"):
        yield


def wait_for(predicate: Callable[[], bool], timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.1)


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
