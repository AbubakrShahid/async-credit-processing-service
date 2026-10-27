import hashlib
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from pymongo import ASCENDING, MongoClient, ReturnDocument
from pymongo.client_session import ClientSession
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

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
    Settlement,
    User,
)


def new_task_id() -> str:
    return f"cr_{uuid.uuid4().hex}"


def request_fingerprint(user_id: str, amount: Decimal) -> str:
    return hashlib.sha256(f"{user_id}:{amount:f}".encode()).hexdigest()


def settle_user_pipeline(
    outcome: CreditStatus, now: datetime, credit: Decimal | None = None, settled: int = 1
) -> list[Document]:
    """Update pipeline that settles pending credits on the user document.

    Adds `credit` to the balance (creating the field if missing), takes `settled` off
    pending_credits, and derives status: `processing` while other credits are still in
    flight, otherwise the outcome of this settlement.
    """
    remaining = {"$subtract": [{"$ifNull": ["$pending_credits", 0]}, settled]}
    settle: Document = {
        "pending_credits": {"$max": [0, remaining]},
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

        Snapshot reads with majority commits: a transaction never decides based on data a
        failover could roll back, and its writes are durable once it returns.
        """
        with self._client.start_session() as session:
            return session.with_transaction(
                callback,
                read_concern=ReadConcern("snapshot"),
                write_concern=WriteConcern("majority", j=True),
            )

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
            "parked_at": None,
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
        """Mark an execution as in flight; None if the credit is settled or parked.

        A crash is detected when RabbitMQ redelivers a message (the previous consumer died
        without acking) and the previous execution never finished. A concurrent duplicate
        is a fresh delivery, so it is not mistaken for a crash.
        """
        crashed = {"$cond": [{"$ifNull": ["$in_flight_since", False]}, 1, 0]} if redelivered else 0
        now = datetime.now(UTC)
        doc = self._operations.find_one_and_update(
            {"_id": task_id, "status": CreditStatus.PROCESSING, "parked_at": None},
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

    def park_credit(self, task_id: str, reason: str) -> None:
        """Stop automatic processing of a credit that keeps crashing. It stays owed."""
        self._operations.update_one(
            {"_id": task_id, "status": CreditStatus.PROCESSING},
            {
                "$set": {
                    "parked_at": datetime.now(UTC),
                    "error": reason,
                    "in_flight_since": None,
                    "updated_at": datetime.now(UTC),
                }
            },
        )

    def unpark_credit(self, task_id: str) -> bool:
        """Hand a parked credit back to the dispatcher with a fresh crash budget."""
        now = datetime.now(UTC)
        result = self._operations.update_one(
            {"_id": task_id, "status": CreditStatus.PROCESSING, "parked_at": {"$ne": None}},
            {
                "$set": {
                    "parked_at": None,
                    "crashes": 0,
                    "error": None,
                    "published_at": None,
                    "next_dispatch_at": now,
                    "updated_at": now,
                }
            },
        )
        return result.modified_count == 1

    def apply_credit(self, task_id: str, *, batch_limit: int = 1) -> Settlement:
        """Apply a pending credit, at most once, with up to `batch_limit - 1` other pending
        credits of the same user in the same transaction (group commit).

        Each credit's `processing -> processed` transition commits together with the
        balance update, so a redelivered message finds its credit settled and changes
        nothing: a worker crash between commit and ack cannot double-credit.

        Batching exists because every credit writes the same user document: settled one
        transaction each, a busy account's transactions collide and retry. Settled
        together, N credits cost one user-document write. Credits are summed in Decimal
        and their order doesn't matter, because credits only add.
        """

        def txn(session: ClientSession) -> Settlement:
            now = datetime.now(UTC)
            trigger = self._operations.find_one(
                {"_id": task_id}, {"user_id": True, "status": True, "amount": True}, session=session
            )
            if trigger is None:
                return Settlement(ApplyOutcome.NOT_FOUND, 0)
            if trigger["status"] != CreditStatus.PROCESSING:
                return Settlement(ApplyOutcome.ALREADY_SETTLED, 0)

            batch = [trigger]
            if batch_limit > 1:
                others = self._operations.find(
                    {
                        "user_id": trigger["user_id"],
                        "status": CreditStatus.PROCESSING,
                        "parked_at": None,
                        "_id": {"$ne": task_id},
                    },
                    {"amount": True},
                    session=session,
                )
                batch += others.sort("created_at", ASCENDING).limit(batch_limit - 1)
            ids = [credit["_id"] for credit in batch]

            settled = self._operations.update_many(
                {"_id": {"$in": ids}, "status": CreditStatus.PROCESSING},
                {
                    "$set": {
                        "status": CreditStatus.PROCESSED,
                        "processed_at": now,
                        "updated_at": now,
                        "in_flight_since": None,
                        "error": None,
                        "settled_by": task_id,
                    }
                },
                session=session,
            )
            # Snapshot isolation: if another transaction settled any of these credits
            # after our snapshot, this write aborts with a write conflict and the whole
            # transaction is retried; it never silently skips a credit.
            if settled.modified_count != len(ids):
                raise RuntimeError(f"settled {settled.modified_count} of {len(ids)} credits")

            total = sum((credit["amount"] for credit in batch), Decimal("0.00"))
            result = self._users.update_one(
                {"user_id": trigger["user_id"]},
                settle_user_pipeline(CreditStatus.PROCESSED, now, credit=total, settled=len(ids)),
                session=session,
            )
            if result.matched_count == 0:
                raise UserNotFoundError(trigger["user_id"])
            return Settlement(ApplyOutcome.APPLIED, len(ids))

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
        due: Document = {
            "status": CreditStatus.PROCESSING,
            "parked_at": None,
            "next_dispatch_at": {"$lte": now},
        }
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
        unpublished: Document = {**processing, "published_at": None, "parked_at": None}

        def oldest_age(query: Document) -> float:
            doc = self._operations.find_one(query, {"created_at": True}, sort=[("created_at", 1)])
            return (now - doc["created_at"]).total_seconds() if doc else 0.0

        return BacklogStats(
            processing=self._operations.count_documents(processing),
            unpublished=self._operations.count_documents(unpublished),
            parked=self._operations.count_documents({**processing, "parked_at": {"$ne": None}}),
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
