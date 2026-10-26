"""Full path: HTTP -> MongoDB -> RabbitMQ -> Celery worker -> MongoDB."""

import time
from collections.abc import Callable, Iterator
from decimal import Decimal

import pytest
from celery.contrib.testing.worker import start_worker
from fastapi.testclient import TestClient
from pymongo.database import Database

from credit_service.api.app import app
from credit_service.db import Document
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
