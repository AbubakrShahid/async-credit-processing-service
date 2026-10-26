from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from fastapi.testclient import TestClient
from pymongo.database import Database

from credit_service.db import Document
from credit_service.services import CreditService


def test_retry_with_same_key_returns_original_credit(
    api: TestClient,
    db: Database[Document],
    make_user: Callable[..., str],
    published: list[str],
) -> None:
    make_user("u_123")
    body = {"user_id": "u_123", "amount": "50.00"}
    headers = {"Idempotency-Key": "k1"}

    first = api.post("/credits", json=body, headers=headers)
    second = api.post("/credits", json=body, headers=headers)

    assert first.status_code == second.status_code == 202
    assert first.json()["task_id"] == second.json()["task_id"]
    assert db.credit_operations.count_documents({}) == 1
    assert published == [first.json()["task_id"]]
    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert user["pending_credits"] == 1


def test_same_key_with_different_body_is_a_conflict(
    api: TestClient, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123")
    headers = {"Idempotency-Key": "k1"}

    api.post("/credits", json={"user_id": "u_123", "amount": "50.00"}, headers=headers)
    response = api.post("/credits", json={"user_id": "u_123", "amount": "60.00"}, headers=headers)

    assert response.status_code == 409
    assert db.credit_operations.count_documents({}) == 1


def test_requests_without_key_are_independent(
    api: TestClient, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123")

    for _ in range(2):
        api.post("/credits", json={"user_id": "u_123", "amount": "50.00"})

    assert db.credit_operations.count_documents({}) == 2


def test_concurrent_requests_with_same_key_create_one_credit(
    service: CreditService,
    db: Database[Document],
    make_user: Callable[..., str],
    published: list[str],
) -> None:
    make_user("u_123")

    with ThreadPoolExecutor(max_workers=8) as pool:
        operations = list(
            pool.map(lambda _: service.submit_credit("u_123", Decimal("5"), "k1"), range(8))
        )

    assert len({op.task_id for op in operations}) == 1
    assert db.credit_operations.count_documents({}) == 1
    assert len(published) == 1
    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert user["pending_credits"] == 1
