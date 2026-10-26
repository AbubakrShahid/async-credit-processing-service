from collections.abc import Callable, Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ServerSelectionTimeoutError

from credit_service.api.app import app
from credit_service.api.dependencies import get_credit_service
from credit_service.config import Settings, get_settings
from credit_service.db import Document, create_client, ensure_indexes, get_database
from credit_service.repository import CreditRepository
from credit_service.services import CreditService, Enqueue, QueueDepth


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "/integration/" in str(item.path):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def settings() -> Settings:
    return get_settings()


@pytest.fixture(scope="session")
def mongo_client(settings: Settings) -> Iterator[MongoClient[Document]]:
    client = create_client(settings)
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError:
        pytest.fail("MongoDB is not reachable - start the stack with `make up` first.")
    yield client
    client.drop_database(settings.mongo_db)
    client.close()


@pytest.fixture
def db(mongo_client: MongoClient[Document], settings: Settings) -> Database[Document]:
    database = get_database(mongo_client, settings)
    for name in database.list_collection_names():
        database[name].delete_many({})
    ensure_indexes(database)
    return database


@pytest.fixture
def repository(
    mongo_client: MongoClient[Document], db: Database[Document], settings: Settings
) -> CreditRepository:
    return CreditRepository(mongo_client, db, settings)


@pytest.fixture
def published() -> list[str]:
    """Task ids the service published, instead of sending them to RabbitMQ."""
    return []


@pytest.fixture
def make_service(
    mongo_client: MongoClient[Document], db: Database[Document], settings: Settings
) -> Callable[..., CreditService]:
    """Build a service with a custom publisher, queue depth probe or settings overrides."""

    def _make_service(
        enqueue: Enqueue = lambda _: None,
        queue_depth: QueueDepth = lambda: None,
        **overrides: Any,
    ) -> CreditService:
        custom = settings.model_copy(update=overrides)
        repository = CreditRepository(mongo_client, db, custom)
        return CreditService(repository, enqueue, custom, queue_depth)

    return _make_service


@pytest.fixture
def service(make_service: Callable[..., CreditService], published: list[str]) -> CreditService:
    return make_service(published.append)


@pytest.fixture
def api(service: CreditService) -> Iterator[TestClient]:
    app.dependency_overrides[get_credit_service] = lambda: service
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture
def make_user(db: Database[Document]) -> Callable[..., str]:
    def _make_user(user_id: str = "u_123", **fields: Any) -> str:
        db.users.insert_one({"user_id": user_id, **fields})
        return user_id

    return _make_user
