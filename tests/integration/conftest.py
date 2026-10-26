from collections.abc import Iterator

import pytest
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ServerSelectionTimeoutError

from credit_service.config import Settings, get_settings
from credit_service.db import Document, create_client, ensure_indexes, get_database


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
