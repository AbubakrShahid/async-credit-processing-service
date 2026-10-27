from decimal import Decimal
from typing import Any

from bson import Decimal128
from bson.codec_options import CodecOptions, TypeCodec, TypeRegistry
from pymongo import ASCENDING, MongoClient
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, ExecutionTimeout, PyMongoError, WTimeoutError

from credit_service.config import Settings

type Document = dict[str, Any]


class DecimalCodec(TypeCodec):
    """Store Python Decimals as BSON Decimal128 and read them back as Decimals."""

    python_type = Decimal
    bson_type = Decimal128

    def transform_python(self, value: Decimal) -> Decimal128:
        return Decimal128(value)

    def transform_bson(self, value: Decimal128) -> Decimal:
        return value.to_decimal()


CODEC_OPTIONS: CodecOptions[Document] = CodecOptions(
    tz_aware=True, type_registry=TypeRegistry([DecimalCodec()])
)


TRANSIENT_ERROR_LABELS = (
    "TransientTransactionError",
    "UnknownTransactionCommitResult",
    "RetryableWriteError",
)


def is_transient(exc: BaseException) -> bool:
    """Errors worth retrying: network blips, failovers, timeouts, write conflicts."""
    if isinstance(exc, ConnectionFailure | ExecutionTimeout | WTimeoutError):
        return True
    return isinstance(exc, PyMongoError) and any(
        exc.has_error_label(label) for label in TRANSIENT_ERROR_LABELS
    )


def create_client(settings: Settings) -> MongoClient[Document]:
    return MongoClient(
        settings.mongo_uri,
        serverSelectionTimeoutMS=settings.mongo_timeout_ms,
        timeoutMS=settings.mongo_operation_timeout_ms,
        tz_aware=True,
        # Pinned rather than inherited from the cluster default: a credit is acknowledged
        # only once a majority has journaled it, so it survives a failover.
        w="majority",
        journal=True,
    )


def get_database(client: MongoClient[Document], settings: Settings) -> Database[Document]:
    return client.get_database(settings.mongo_db, codec_options=CODEC_OPTIONS)


def ensure_indexes(db: Database[Document]) -> None:
    db.users.create_index([("user_id", ASCENDING)], unique=True)
    # Idempotency keys are scoped to the API client that sent them.
    db.credit_operations.create_index(
        [("client_id", ASCENDING), ("idempotency_key", ASCENDING)],
        unique=True,
        partialFilterExpression={"idempotency_key": {"$type": "string"}},
    )
    # Dispatcher claims, oldest first.
    db.credit_operations.create_index([("status", ASCENDING), ("next_dispatch_at", ASCENDING)])
    # Group commit: a user's pending credits, oldest first.
    db.credit_operations.create_index(
        [("user_id", ASCENDING), ("status", ASCENDING), ("created_at", ASCENDING)]
    )
    # Backlog metrics: oldest pending credit.
    db.credit_operations.create_index([("status", ASCENDING), ("created_at", ASCENDING)])
