import hashlib
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from pymongo import ASCENDING, MongoClient, ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from credit_service.config import Settings
from credit_service.db import Document
from credit_service.errors import (
    IdempotencyConflictError,
    TooManyPendingCreditsError,
    UserNotFoundError,
)
from credit_service.models import (
    ApplyOutcome,
    AttemptState,
    BacklogStats,
    CreditOperation,
    CreditStatus,
    User,
)


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
    def __init__(
        self, client: MongoClient[Document], db: Database[Document], settings: Settings
    ) -> None:
        self._client = client
        self._users = db.users
        self._operations = db.credit_operations
        self._publish_grace = timedelta(seconds=settings.publish_grace_seconds)
        self._republish_after = timedelta(seconds=settings.republish_after_seconds)
        self._dispatch_lease = timedelta(seconds=settings.dispatch_lease_seconds)

    def _transaction[T](self, callback: Callable[[ClientSession], T]) -> T:
        """Run `callback` in a transaction.

        with_transaction reruns the callback on TransientTransactionError but retries only
        the commit on UnknownTransactionCommitResult, so a commit whose reply was lost is
        never executed twice. If the outcome is still unknown when its retry window closes,
        the error propagates; callers treat it as transient, and every write here is a
        guarded state transition, so running the operation again is safe.
        """
        with self._client.start_session() as session:
            return session.with_transaction(callback)

    # --- accepting credits -------------------------------------------------------------

    def create_operation(
        self,
        user_id: str,
        amount: Decimal,
        *,
        client_id: str,
        idempotency_key: str | None = None,
        max_pending: int,
    ) -> tuple[CreditOperation, bool]:
        """Record a pending credit and mark the user as processing, atomically.

        Returns the operation and whether it was created. With an idempotency key that
        this client already used for the same request, the original operation is returned.
        """
        fingerprint = request_fingerprint(user_id, amount)
        if idempotency_key is not None:
            existing = self._find_by_idempotency_key(client_id, idempotency_key, fingerprint)
            if existing is not None:
                return existing, False

        now = datetime.now(UTC)
        doc: Document = {
            "_id": new_task_id(),
            "client_id": client_id,
            "user_id": user_id,
            "amount": amount,
            "status": CreditStatus.PROCESSING,
            "attempts": 0,
            "crashes": 0,
            "in_flight_since": None,
            "error": None,
            "created_at": now,
            "updated_at": now,
            "processed_at": None,
            # Dispatch state: the API publishes right away; the dispatcher only steps in
            # if that publish isn't confirmed within the grace period.
            "published_at": None,
            "dispatch_attempts": 0,
            "next_dispatch_at": now + self._publish_grace,
        }
        if idempotency_key is not None:
            doc |= {"idempotency_key": idempotency_key, "request_fingerprint": fingerprint}

        def txn(session: ClientSession) -> None:
            result = self._users.update_one(
                {"user_id": user_id, "pending_credits": {"$not": {"$gte": max_pending}}},
                {
                    "$inc": {"pending_credits": 1},
                    "$set": {"status": CreditStatus.PROCESSING, "updated_at": now},
                },
                session=session,
            )
            if result.matched_count == 0:
                if self._users.count_documents({"user_id": user_id}, session=session):
                    raise TooManyPendingCreditsError(user_id)
                raise UserNotFoundError(user_id)
            self._operations.insert_one(doc, session=session)

        try:
            self._transaction(txn)
        except DuplicateKeyError:
            # A concurrent request with the same idempotency key committed first.
            if idempotency_key is None:
                raise
            existing = self._find_by_idempotency_key(client_id, idempotency_key, fingerprint)
            if existing is None:
                raise
            return existing, False
        return CreditOperation.from_document(doc), True

    def _find_by_idempotency_key(
        self, client_id: str, key: str, fingerprint: str
    ) -> CreditOperation | None:
        doc = self._operations.find_one({"client_id": client_id, "idempotency_key": key})
        if doc is None:
            return None
        if doc["request_fingerprint"] != fingerprint:
            raise IdempotencyConflictError(key)
        return CreditOperation.from_document(doc)

    # --- processing credits ------------------------------------------------------------

    def start_attempt(self, task_id: str, *, redelivered: bool) -> AttemptState | None:
        """Mark an execution as in flight; None if the credit is no longer pending.

        A crash is detected when RabbitMQ redelivers a message (the previous consumer died
        without acking) and the previous execution never finished. A concurrent duplicate
        is a fresh delivery, so it is not mistaken for a crash.
        """
        crashed = {"$cond": [{"$ifNull": ["$in_flight_since", False]}, 1, 0]} if redelivered else 0
        now = datetime.now(UTC)
        doc = self._operations.find_one_and_update(
            {"_id": task_id, "status": CreditStatus.PROCESSING},
            [
                {
                    "$set": {
                        "attempts": {"$add": [{"$ifNull": ["$attempts", 0]}, 1]},
                        "crashes": {"$add": [{"$ifNull": ["$crashes", 0]}, crashed]},
                        "in_flight_since": now,
                        "updated_at": now,
                    }
                }
            ],
            projection={"attempts": True, "crashes": True},
            return_document=ReturnDocument.AFTER,
        )
        return AttemptState(doc["attempts"], doc["crashes"]) if doc else None

    def end_attempt(self, task_id: str, error: str, *, crashed: bool) -> None:
        """Close an execution that raised, recording the error (and a crash if unexpected)."""
        update: Document = {
            "$set": {"in_flight_since": None, "error": error, "updated_at": datetime.now(UTC)}
        }
        if crashed:
            update["$inc"] = {"crashes": 1}
        self._operations.update_one({"_id": task_id, "status": CreditStatus.PROCESSING}, update)

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
                        "in_flight_since": None,
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

        return self._transaction(txn)

    def fail_credit(self, task_id: str, error: str) -> bool:
        """Mark a pending credit failed and release it from the user's pending count.

        Conditional on `processing`, so it can never override a credit that another
        execution has already processed, and never decrements pending_credits twice.
        """

        def txn(session: ClientSession) -> bool:
            now = datetime.now(UTC)
            operation = self._operations.find_one_and_update(
                {"_id": task_id, "status": CreditStatus.PROCESSING},
                {
                    "$set": {
                        "status": CreditStatus.FAILED,
                        "error": error,
                        "in_flight_since": None,
                        "updated_at": now,
                    }
                },
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

        return self._transaction(txn)

    # --- dispatching ---------------------------------------------------------------------

    def mark_published(self, task_ids: list[str]) -> None:
        """Record broker-confirmed publishes; such credits are only reconciled much later."""
        if not task_ids:
            return
        now = datetime.now(UTC)
        self._operations.update_many(
            {"_id": {"$in": task_ids}},
            {
                "$set": {"published_at": now, "next_dispatch_at": now + self._republish_after},
                "$unset": {"dispatch_token": ""},
            },
        )

    def claim_for_dispatch(self, limit: int, *, include_published: bool) -> list[str]:
        """Claim up to `limit` pending credits that are due for (re)publishing.

        Oldest first, so steady new traffic cannot starve old credits. The claim stamps a
        unique token and pushes next_dispatch_at out by the lease, so concurrent
        dispatchers never publish the same credit in the same round, and a dispatcher that
        dies mid-batch only delays its claims until the lease runs out.
        """
        now = datetime.now(UTC)
        due: Document = {"status": CreditStatus.PROCESSING, "next_dispatch_at": {"$lte": now}}
        if not include_published:
            due["published_at"] = None
        candidates = [
            doc["_id"]
            for doc in self._operations.find(due, {"_id": True})
            .sort("next_dispatch_at", ASCENDING)
            .limit(limit)
        ]
        if not candidates:
            return []
        token = uuid.uuid4().hex
        self._operations.update_many(
            {**due, "_id": {"$in": candidates}},
            {
                "$set": {"next_dispatch_at": now + self._dispatch_lease, "dispatch_token": token},
                "$inc": {"dispatch_attempts": 1},
            },
        )
        claimed = self._operations.find(
            {"_id": {"$in": candidates}, "dispatch_token": token}, {"_id": True}
        )
        return [doc["_id"] for doc in claimed]

    def release_claims(self, task_ids: list[str]) -> None:
        """Make claimed-but-unpublished credits due again (e.g. the broker went away)."""
        if task_ids:
            self._operations.update_many(
                {"_id": {"$in": task_ids}, "status": CreditStatus.PROCESSING},
                {"$set": {"next_dispatch_at": datetime.now(UTC)}, "$unset": {"dispatch_token": ""}},
            )

    def backlog_stats(self) -> BacklogStats:
        now = datetime.now(UTC)
        processing: Document = {"status": CreditStatus.PROCESSING}
        unpublished: Document = {**processing, "published_at": None}

        def oldest_age(query: Document) -> float:
            doc = self._operations.find_one(query, {"created_at": True}, sort=[("created_at", 1)])
            return (now - doc["created_at"]).total_seconds() if doc else 0.0

        return BacklogStats(
            processing=self._operations.count_documents(processing),
            unpublished=self._operations.count_documents(unpublished),
            failed=self._operations.count_documents({"status": CreditStatus.FAILED}),
            oldest_processing_age_seconds=oldest_age(processing),
            oldest_unpublished_age_seconds=oldest_age(unpublished),
        )

    # --- reads -------------------------------------------------------------------------

    def get_operation(self, task_id: str) -> CreditOperation | None:
        doc = self._operations.find_one({"_id": task_id})
        return CreditOperation.from_document(doc) if doc else None

    def get_user(self, user_id: str) -> User | None:
        doc = self._users.find_one({"user_id": user_id})
        return User.from_document(doc) if doc else None
