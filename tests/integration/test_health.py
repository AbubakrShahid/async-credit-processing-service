from fastapi.testclient import TestClient
from pymongo import MongoClient


def test_liveness(api: TestClient) -> None:
    assert api.get("/health/live").json() == {"status": "ok"}


def test_readiness_checks_mongo_and_rabbitmq(api: TestClient) -> None:
    response = api.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "checks": {"mongodb": "ok", "rabbitmq": "ok"}}


def test_readiness_fails_when_mongo_is_unreachable(api: TestClient) -> None:
    state = api.app.state
    healthy_client = state.mongo_client
    state.mongo_client = MongoClient("mongodb://localhost:1", serverSelectionTimeoutMS=200)
    try:
        response = api.get("/health/ready")
    finally:
        state.mongo_client.close()
        state.mongo_client = healthy_client

    assert response.status_code == 503
    assert response.json()["checks"]["mongodb"] == "error: ServerSelectionTimeoutError"
