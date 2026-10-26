import uuid
from datetime import UTC, datetime
from decimal import Decimal

from pymongo import MongoClient
from pymongo.client_session import ClientSession
from pymongo.database import Database

from credit_service.db import Document
from credit_service.errors import UserNotFoundError
from credit_service.models import CreditOperation, CreditStatus


def new_task_id() -> str:
    return f"cr_{uuid.uuid4().hex}"


class CreditRepository:
    def __init__(self, client: MongoClient[Document], db: Database[Document]) -> None:
        self._client = client
        self._users = db.users
        self._operations = db.credit_operations

    def create_operation(self, user_id: str, amount: Decimal) -> CreditOperation:
        """Record a pending credit and mark the user as processing, atomically."""
        now = datetime.now(UTC)
        doc: Document = {
            "_id": new_task_id(),
            "user_id": user_id,
            "amount": amount,
            "status": CreditStatus.PROCESSING.value,
            "attempts": 0,
            "error": None,
            "created_at": now,
            "updated_at": now,
            "processed_at": None,
        }

        def txn(session: ClientSession) -> None:
            result = self._users.update_one(
                {"user_id": user_id},
                {
                    "$inc": {"pending_credits": 1},
                    "$set": {"status": CreditStatus.PROCESSING.value, "updated_at": now},
                },
                session=session,
            )
            if result.matched_count == 0:
                raise UserNotFoundError(user_id)
            self._operations.insert_one(doc, session=session)

        with self._client.start_session() as session:
            session.with_transaction(txn)
        return CreditOperation.from_document(doc)

    def get_operation(self, task_id: str) -> CreditOperation | None:
        doc = self._operations.find_one({"_id": task_id})
        return CreditOperation.from_document(doc) if doc else None
