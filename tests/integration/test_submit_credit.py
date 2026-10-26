from collections.abc import Callable
from decimal import Decimal

from fastapi.testclient import TestClient
from pymongo.database import Database

from credit_service.db import Document
from credit_service.repository import CreditRepository
from credit_service.services import CreditService


def test_accepts_credit_and_publishes_task(
    api: TestClient,
    db: Database[Document],
    make_user: Callable[..., str],
    published: list[str],
) -> None:
    make_user("u_123")

    response = api.post("/credits", json={"user_id": "u_123", "amount": "50.00"})

    assert response.status_code == 202
    body = response.json()
    task_id = body["task_id"]
    assert body == {
        "task_id": task_id,
        "status": "processing",
        "status_url": f"/credits/{task_id}",
    }
    assert response.headers["Location"] == f"/credits/{task_id}"
    assert published == [task_id]

    operation = db.credit_operations.find_one({"_id": task_id})
    assert operation is not None
    assert operation["amount"] == Decimal("50.00")
    assert operation["status"] == "processing"

    user = db.users.find_one({"user_id": "u_123"})
    assert user is not None
    assert user["status"] == "processing"
    assert user["pending_credits"] == 1


def test_unknown_user_is_rejected_without_side_effects(
    api: TestClient, db: Database[Document], published: list[str]
) -> None:
    response = api.post("/credits", json={"user_id": "u_missing", "amount": "10"})

    assert response.status_code == 404
    assert db.credit_operations.count_documents({}) == 0
    assert published == []


def test_invalid_amount_is_rejected(api: TestClient, make_user: Callable[..., str]) -> None:
    make_user("u_123")

    for amount in ["-1", "0", "1.234", "abc", None]:
        response = api.post("/credits", json={"user_id": "u_123", "amount": amount})
        assert response.status_code == 422, amount


def test_request_is_accepted_when_broker_publish_fails(
    repository: CreditRepository, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123")

    def broken_enqueue(_: str) -> None:
        raise ConnectionError("broker down")

    operation = CreditService(repository, broken_enqueue).submit_credit("u_123", Decimal("5"))

    assert db.credit_operations.find_one({"_id": operation.task_id}) is not None
