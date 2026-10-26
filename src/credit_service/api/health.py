from typing import Any

from fastapi import APIRouter, Request, Response, status
from pymongo import MongoClient

from credit_service.celery_app import celery_app

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live")
def live() -> dict[str, str]:
    """The process is up. Deliberately checks no dependencies."""
    return {"status": "ok"}


@router.get("/ready")
def ready(request: Request, response: Response) -> dict[str, Any]:
    """Whether this instance can accept credits: MongoDB and RabbitMQ are reachable."""
    checks = {"mongodb": _check_mongo(request.app.state.mongo_client), "rabbitmq": _check_broker()}
    healthy = all(result == "ok" for result in checks.values())
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ok" if healthy else "unavailable", "checks": checks}


def _check_mongo(client: MongoClient[Any]) -> str:
    try:
        client.admin.command("ping")
    except Exception as exc:
        return f"error: {type(exc).__name__}"
    return "ok"


def _check_broker() -> str:
    try:
        with celery_app.connection_for_write() as connection:
            connection.ensure_connection(max_retries=1, timeout=2)
    except Exception as exc:
        return f"error: {type(exc).__name__}"
    return "ok"
