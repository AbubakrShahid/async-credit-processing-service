from collections.abc import Callable
from decimal import Decimal

from fastapi.testclient import TestClient

from credit_service.services import CreditService


def test_credit_status_follows_processing(
    api: TestClient, service: CreditService, make_user: Callable[..., str]
) -> None:
    make_user("u_123")
    task_id = api.post("/credits", json={"user_id": "u_123", "amount": "50"}).json()["task_id"]

    pending = api.get(f"/credits/{task_id}").json()
    assert pending["status"] == "processing"
    assert pending["amount"] == "50.00"
    assert pending["processed_at"] is None

    service.apply_credit(task_id)

    done = api.get(f"/credits/{task_id}").json()
    assert done["status"] == "processed"
    assert done["processed_at"] is not None


def test_user_balance(api: TestClient, make_user: Callable[..., str]) -> None:
    make_user("u_123", balance=Decimal("150.5"), status="processed", pending_credits=0)

    assert api.get("/users/u_123").json() == {
        "user_id": "u_123",
        "balance": "150.50",
        "status": "processed",
        "pending_credits": 0,
    }


def test_user_without_balance_reports_zero(api: TestClient, make_user: Callable[..., str]) -> None:
    make_user("u_new")

    body = api.get("/users/u_new").json()
    assert (body["balance"], body["status"]) == ("0.00", None)


def test_unknown_ids_are_404(api: TestClient) -> None:
    assert api.get("/credits/cr_nope").status_code == 404
    assert api.get("/users/u_nope").status_code == 404
