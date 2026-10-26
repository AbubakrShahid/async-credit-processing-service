import uuid
from datetime import UTC, datetime
from decimal import Decimal

from pymongo import MongoClient, ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.database import Database

from credit_service.db import Document
from credit_service.errors import UserNotFoundError
from credit_service.models import ApplyOutcome, CreditOperation, CreditStatus


def new_task_id() -> str:
    return f"cr_{uuid.uuid4().hex}"


def settle_user_pipeline(
    outcome: CreditStatus, now: datetime, credit: Decimal | None = None
) -> list[Document]:
    """Update pipeline that settles one pending credit on the user document.

    Adds the credit to the balance (creating the field if missing), decrements
    pending_credits, and derives status: `processing` while other credits are still in
    flight, otherwise the outcome of this operation.
    """
    settle: Document = {
        "pending_credits": {"$max": [0, {"$subtract": [{"$ifNull": ["$pending_credits", 0]}, 1]}]},
        "updated_at": now,
    }
    if credit is not None:
        settle["balance"] = {"$add": [{"$ifNull": ["$balance", Decimal("0.00")]}, credit]}
    status = {"$cond": [{"$gt": ["$pending_credits", 0]}, CreditStatus.PROCESSING, outcome]}
    return [{"$set": settle}, {"$set": {"status": status}}]


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
            "status": CreditStatus.PROCESSING,
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
                    "$set": {"status": CreditStatus.PROCESSING, "updated_at": now},
                },
                session=session,
            )
            if result.matched_count == 0:
                raise UserNotFoundError(user_id)
            self._operations.insert_one(doc, session=session)

        with self._client.start_session() as session:
            session.with_transaction(txn)
        return CreditOperation.from_document(doc)

    def apply_credit(self, task_id: str) -> ApplyOutcome:
        """Apply a pending credit to the user's balance at most once.

        The operation's `processing -> processed` transition and the balance update
        commit together. A redelivered message finds the operation already settled and
        changes nothing, so a worker crash between commit and ack cannot double-credit.
        """

        def txn(session: ClientSession) -> ApplyOutcome:
            now = datetime.now(UTC)
            operation = self._operations.find_one_and_update(
                {"_id": task_id, "status": CreditStatus.PROCESSING},
                {
                    "$set": {
                        "status": CreditStatus.PROCESSED,
                        "processed_at": now,
                        "updated_at": now,
                        "error": None,
                    }
                },
                return_document=ReturnDocument.AFTER,
                session=session,
            )
            if operation is None:
                exists = self._operations.count_documents({"_id": task_id}, session=session)
                return ApplyOutcome.ALREADY_SETTLED if exists else ApplyOutcome.NOT_FOUND

            result = self._users.update_one(
                {"user_id": operation["user_id"]},
                settle_user_pipeline(CreditStatus.PROCESSED, now, credit=operation["amount"]),
                session=session,
            )
            if result.matched_count == 0:
                raise UserNotFoundError(operation["user_id"])
            return ApplyOutcome.APPLIED

        with self._client.start_session() as session:
            return session.with_transaction(txn)

    def get_operation(self, task_id: str) -> CreditOperation | None:
        doc = self._operations.find_one({"_id": task_id})
        return CreditOperation.from_document(doc) if doc else None
