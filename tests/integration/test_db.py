from decimal import Decimal

import pytest
from bson import Decimal128
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from credit_service.db import Document


def test_decimals_are_stored_as_decimal128(db: Database[Document]) -> None:
    db.users.insert_one({"user_id": "u_1", "balance": Decimal("10.10")})

    raw = db.users.with_options(codec_options=db.codec_options.with_options(type_registry=None))
    assert raw.find_one({"user_id": "u_1"})["balance"] == Decimal128("10.10")
    assert db.users.find_one({"user_id": "u_1"})["balance"] == Decimal("10.10")


def test_user_id_is_unique(db: Database[Document]) -> None:
    db.users.insert_one({"user_id": "u_1"})
    with pytest.raises(DuplicateKeyError):
        db.users.insert_one({"user_id": "u_1"})


def test_writes_require_majority_journaled_acknowledgement(
    mongo_client: MongoClient[Document],
) -> None:
    # Durability must not depend on whatever default the cluster happens to have.
    assert mongo_client.write_concern.document == {"w": "majority", "j": True}
