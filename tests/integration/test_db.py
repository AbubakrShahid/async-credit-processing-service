from decimal import Decimal

import pytest
from bson import Decimal128
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from credit_service.db import Document


def test_decimals_are_stored_as_decimal128(db: Database[Document]) -> None:
    db.users.insert_one({"user_id": "u_1", "balance": Decimal("10.10")})

    raw = db.users.with_options(codec_options=db.codec_options.with_options(type_registry=None))
    assert raw.find_one({"user_id": "u_1"})["balance"] == Decimal128("10.10")  # type: ignore[index]
    assert db.users.find_one({"user_id": "u_1"})["balance"] == Decimal("10.10")  # type: ignore[index]


def test_user_id_is_unique(db: Database[Document]) -> None:
    db.users.insert_one({"user_id": "u_1"})
    with pytest.raises(DuplicateKeyError):
        db.users.insert_one({"user_id": "u_1"})
