"""Authentication, admission control and safe retries at the API boundary."""

from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pymongo.database import Database
from pymongo.errors import OperationFailure

from credit_service.api.app import app
from credit_service.api.dependencies import get_credit_service
from credit_service.db import Document
from credit_service.repository import CreditRepository
from credit_service.services import CreditService

CREDIT = {"user_id": "u_123", "amount": "10.00"}


@pytest.fixture
def with_api_keys(api: TestClient) -> Iterator[None]:
    app.state.api_key_clients = {"key-a": "client_a", "key-b": "client_b"}
    yield
    app.state.api_key_clients = {}


def use_service(service: CreditService) -> None:
    app.dependency_overrides[get_credit_service] = lambda: service


def test_api_key_is_required_when_configured(
    api: TestClient, with_api_keys: None, make_user: Callable[..., str]
) -> None:
    make_user("u_123")

    assert api.post("/credits", json=CREDIT).status_code == 401
    assert api.post("/credits", json=CREDIT, headers={"X-API-Key": "nope"}).status_code == 401
    assert api.get("/users/u_123").status_code == 401
    assert api.post("/credits", json=CREDIT, headers={"X-API-Key": "key-a"}).status_code == 202
    # Probes and metrics stay reachable for the platform.
    assert api.get("/health/live").status_code == 200
    assert api.get("/metrics").status_code == 200


def test_idempotency_keys_are_scoped_per_client(
    api: TestClient, with_api_keys: None, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123")

    first = api.post(
        "/credits", json=CREDIT, headers={"X-API-Key": "key-a", "Idempotency-Key": "k"}
    )
    other = api.post(
        "/credits", json=CREDIT, headers={"X-API-Key": "key-b", "Idempotency-Key": "k"}
    )

    assert first.json()["task_id"] != other.json()["task_id"]
    assert db.credit_operations.count_documents({}) == 2


def test_too_many_pending_credits_for_one_user_is_429(
    api: TestClient,
    make_service: Callable[..., CreditService],
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    use_service(make_service(max_pending_per_user=2))
    make_user("u_123")

    statuses = [api.post("/credits", json=CREDIT).status_code for _ in range(3)]

    assert statuses == [202, 202, 429]
    assert db.credit_operations.count_documents({}) == 2  # nothing committed for the 429


def test_backlogged_queue_rejects_before_committing(
    api: TestClient,
    make_service: Callable[..., CreditService],
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    use_service(make_service(queue_depth=lambda: 1_000_000))
    make_user("u_123")

    response = api.post("/credits", json=CREDIT)

    assert response.status_code == 503
    assert response.headers["Retry-After"] == "5"
    assert db.credit_operations.count_documents({}) == 0


def test_lost_commit_reply_is_503_and_safe_to_retry(
    api: TestClient,
    make_service: Callable[..., CreditService],
    db: Database[Document],
    make_user: Callable[..., str],
    published: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    make_user("u_123")
    headers = {"Idempotency-Key": "retry-me"}
    real_transaction = CreditRepository._transaction

    def commit_reply_lost(self: CreditRepository, callback: Any) -> Any:
        real_transaction(self, callback)  # the credit IS committed...
        raise OperationFailure(  # ...but the API never hears back
            "connection reset before commit reply",
            details={"errorLabels": ["UnknownTransactionCommitResult"]},
        )

    with monkeypatch.context() as patch:
        patch.setattr(CreditRepository, "_transaction", commit_reply_lost)
        first = api.post("/credits", json=CREDIT, headers=headers)
    assert first.status_code == 503
    assert "retry with the same Idempotency-Key" in first.json()["detail"]

    retry = api.post("/credits", json=CREDIT, headers=headers)
    assert retry.status_code == 202
    assert db.credit_operations.count_documents({}) == 1
    assert published == []  # the API never got to publish it...

    db.credit_operations.update_many(
        {}, {"$set": {"next_dispatch_at": datetime.now(UTC) - timedelta(seconds=1)}}
    )
    dispatched: list[str] = []
    make_service(dispatched.append).dispatch_pending()
    assert dispatched == [retry.json()["task_id"]]  # ...so the dispatcher does


def test_metrics_expose_backlog(
    api: TestClient, service: CreditService, make_user: Callable[..., str]
) -> None:
    make_user("u_123")
    service.submit_credit("u_123", Decimal("1"))

    body = api.get("/metrics").text

    assert "credit_backlog_up 1.0" in body
    assert "credit_processing 1.0" in body
    assert "credit_oldest_processing_age_seconds" in body
