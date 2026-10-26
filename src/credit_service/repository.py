import hashlib
import uuid
from datetime import UTC, datetime
from decimal import Decimal

from pymongo import ASCENDING, MongoClient, ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from credit_service.db import Document
from credit_service.errors import IdempotencyConflictError, UserNotFoundError
from credit_service.models import ApplyOutcome, CreditOperation, CreditStatus, User


def new_task_id() -> str:
    return f"cr_{uuid.uuid4().hex}"


def request_fingerprint(user_id: str, amount: Decimal) -> str:
    return hashlib.sha256(f"{user_id}:{amount:f}".encode()).hexdigest()


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

    def create_operation(
        self, user_id: str, amount: Decimal, idempotency_key: str | None = None
    ) -> tuple[CreditOperation, bool]:
        """Record a pending credit and mark the user as processing, atomically.

        Returns the operation and whether it was created. With an idempotency key that
        was already used for the same request, the original operation is returned instead.
        """
        fingerprint = request_fingerprint(user_id, amount)
        if idempotency_key is not None:
            existing = self._find_by_idempotency_key(idempotency_key, fingerprint)
            if existing is not None:
                return existing, False

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
            "last_enqueued_at": now,
        }
        if idempotency_key is not None:
            doc |= {"idempotency_key": idempotency_key, "request_fingerprint": fingerprint}

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

        try:
            with self._client.start_session() as session:
                session.with_transaction(txn)
        except DuplicateKeyError:
            # A concurrent request with the same idempotency key committed first.
            if idempotency_key is None:
                raise
            existing = self._find_by_idempotency_key(idempotency_key, fingerprint)
            if existing is None:
                raise
            return existing, False
        return CreditOperation.from_document(doc), True

    def _find_by_idempotency_key(self, key: str, fingerprint: str) -> CreditOperation | None:
        doc = self._operations.find_one({"idempotency_key": key})
        if doc is None:
            return None
        if doc["request_fingerprint"] != fingerprint:
            raise IdempotencyConflictError(key)
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

    def record_attempt(self, task_id: str) -> int | None:
        """Count a processing attempt; None if the credit is not pending (or unknown)."""
        doc = self._operations.find_one_and_update(
            {"_id": task_id, "status": CreditStatus.PROCESSING},
            {"$inc": {"attempts": 1}, "$set": {"updated_at": datetime.now(UTC)}},
            projection={"attempts": True},
            return_document=ReturnDocument.AFTER,
        )
        return doc["attempts"] if doc else None

    def fail_credit(self, task_id: str, error: str) -> bool:
        """Mark a pending credit failed and release it from the user's pending count."""

        def txn(session: ClientSession) -> bool:
            now = datetime.now(UTC)
            operation = self._operations.find_one_and_update(
                {"_id": task_id, "status": CreditStatus.PROCESSING},
                {"$set": {"status": CreditStatus.FAILED, "error": error, "updated_at": now}},
                session=session,
            )
            if operation is None:
                return False
            # The user may be gone, which is often why we are failing; that's fine.
            self._users.update_one(
                {"user_id": operation["user_id"]},
                settle_user_pipeline(CreditStatus.FAILED, now),
                session=session,
            )
            return True

        with self._client.start_session() as session:
            return session.with_transaction(txn)

    def claim_stale_operations(self, enqueued_before: datetime, limit: int) -> list[str]:
        """Claim pending credits not (re)published since `enqueued_before`.

        Each claim bumps last_enqueued_at atomically, so concurrent sweeps never claim
        the same credit twice.
        """
        now = datetime.now(UTC)
        claimed: list[str] = []
        while len(claimed) < limit:
            doc = self._operations.find_one_and_update(
                {"status": CreditStatus.PROCESSING, "last_enqueued_at": {"$lt": enqueued_before}},
                {"$set": {"last_enqueued_at": now}},
                sort=[("last_enqueued_at", ASCENDING)],
                projection={"_id": True},
            )
            if doc is None:
                break
            claimed.append(doc["_id"])
        return claimed

    def get_operation(self, task_id: str) -> CreditOperation | None:
        doc = self._operations.find_one({"_id": task_id})
        return CreditOperation.from_document(doc) if doc else None

    def get_user(self, user_id: str) -> User | None:
        doc = self._users.find_one({"user_id": user_id})
        return User.from_document(doc) if doc else None
