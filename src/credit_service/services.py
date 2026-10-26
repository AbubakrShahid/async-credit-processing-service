import time
from collections.abc import Callable
from decimal import Decimal

import structlog
from pymongo.errors import PyMongoError

from credit_service.config import Settings
from credit_service.errors import CreditNotFoundError, ServiceOverloadedError, UserNotFoundError
from credit_service.models import ApplyOutcome, BacklogStats, CreditOperation, DispatchReport, User
from credit_service.repository import CreditRepository

logger = structlog.get_logger(__name__)

type Enqueue = Callable[[str], None]
type QueueDepth = Callable[[], int | None]

ANONYMOUS_CLIENT = "anonymous"


def _unknown_depth() -> int | None:
    return None


class CreditService:
    """Credit workflows shared by the API and the worker. No FastAPI or Celery in here."""

    def __init__(
        self,
        repository: CreditRepository,
        enqueue: Enqueue,
        settings: Settings,
        queue_depth: QueueDepth = _unknown_depth,
    ) -> None:
        self._repository = repository
        self._enqueue = enqueue
        self._settings = settings
        self._queue_depth = queue_depth
        self._publish_paused_until = 0.0

    # --- accepting -----------------------------------------------------------------------

    def submit_credit(
        self,
        user_id: str,
        amount: Decimal,
        idempotency_key: str | None = None,
        client_id: str = ANONYMOUS_CLIENT,
    ) -> CreditOperation:
        # Admission control happens before anything is committed, so a rejected request
        # leaves no trace and the client can simply retry later.
        depth = self._queue_depth()
        if depth is not None and depth >= self._settings.max_queue_depth:
            raise ServiceOverloadedError()

        operation, created = self._repository.create_operation(
            user_id,
            amount,
            client_id=client_id,
            idempotency_key=idempotency_key,
            max_pending=self._settings.max_pending_per_user,
        )
        log = logger.bind(task_id=operation.task_id, user_id=user_id, client_id=client_id)
        if not created:
            log.info("credit_replayed", idempotency_key=idempotency_key)
            return operation

        # From here on the credit is committed: nothing below may fail the request, or a
        # client without an idempotency key could retry and credit twice.
        if time.monotonic() < self._publish_paused_until:
            log.info("credit_publish_deferred")  # broker recently down; dispatcher publishes
            return operation
        try:
            self._enqueue(operation.task_id)
        except Exception:
            self._publish_paused_until = time.monotonic() + self._settings.publish_pause_seconds
            log.exception("credit_publish_failed")  # the dispatcher publishes it later
            return operation
        try:
            self._repository.mark_published([operation.task_id])
        except PyMongoError:
            # Published but not recorded: the dispatcher may publish it again, harmlessly.
            log.warning("credit_publish_not_recorded", exc_info=True)
        log.info("credit_accepted", amount=str(amount))
        return operation

    # --- processing ----------------------------------------------------------------------

    def apply_credit(self, task_id: str, *, redelivered: bool = False) -> ApplyOutcome:
        """Apply a credit. Transient MongoDB errors propagate so the caller can retry.

        Failure policy: infrastructure errors never fail a credit (it is retried until
        MongoDB is back); a business error fails it immediately; a credit whose processing
        keeps crashing is parked for an operator rather than failed, because repeated crashes
        are as likely to be a restart loop as a poison message, and the money is still owed.
        """
        state = self._repository.start_attempt(task_id, redelivered=redelivered)
        if state is None:
            operation = self._repository.get_operation(task_id)
            if operation is None:
                logger.warning("credit_not_found", task_id=task_id)
                return ApplyOutcome.NOT_FOUND
            parked = operation.parked_at is not None and operation.status == "processing"
            return ApplyOutcome.PARKED if parked else ApplyOutcome.ALREADY_SETTLED
        if state.crashes >= self._settings.max_crashes:
            reason = f"processing crashed {state.crashes} times; parked for review"
            logger.error("credit_parked", task_id=task_id, reason=reason)
            self._repository.park_credit(task_id, reason)
            return ApplyOutcome.PARKED

        try:
            return self._repository.apply_credit(task_id)
        except UserNotFoundError as exc:
            # The user was deleted after the credit was accepted; retrying won't help.
            return self._fail(task_id, str(exc))

    def record_failed_attempt(self, task_id: str, exc: BaseException, *, transient: bool) -> None:
        """Best effort: MongoDB may be the thing that is failing."""
        try:
            self._repository.end_attempt(task_id, repr(exc), crashed=not transient)
        except PyMongoError:
            logger.warning("attempt_not_recorded", task_id=task_id, exc_info=True)

    def unpark_credit(self, task_id: str) -> bool:
        unparked = self._repository.unpark_credit(task_id)
        logger.warning("credit_unparked", task_id=task_id, unparked=unparked)
        return unparked

    def _fail(self, task_id: str, reason: str) -> ApplyOutcome:
        logger.error("credit_failed", task_id=task_id, reason=reason)
        self._repository.fail_credit(task_id, reason)
        return ApplyOutcome.FAILED

    # --- dispatching ---------------------------------------------------------------------

    def dispatch_pending(self) -> DispatchReport:
        """Publish credits whose publish was never confirmed, in bounded batches.

        Runs until nothing is due or the time budget is spent. Confirmed-but-unprocessed
        credits are only republished while the queue is (nearly) drained: a deep queue
        means they are most likely still waiting in it, and republishing would only make
        the backlog worse.
        """
        settings = self._settings
        depth = self._queue_depth()
        reconcile = depth is not None and depth <= settings.reconcile_max_queue_depth
        deadline = time.monotonic() + settings.dispatch_time_budget_seconds
        published = 0

        while time.monotonic() < deadline:
            batch = self._repository.claim_for_dispatch(
                settings.dispatch_batch_size, include_published=reconcile
            )
            if not batch:
                break
            for sent, task_id in enumerate(batch):
                try:
                    self._enqueue(task_id)
                except Exception:
                    # Broker unavailable: stop here and retry on the next scheduled run,
                    # which caps the load on a struggling broker at one attempt per run.
                    logger.exception("dispatch_publish_failed", remaining=len(batch) - sent)
                    self._repository.mark_published(batch[:sent])
                    self._repository.release_claims(batch[sent:])
                    return DispatchReport(published + sent, reconcile, broker_available=False)
            self._repository.mark_published(batch)
            published += len(batch)

        return DispatchReport(published, reconcile, broker_available=True)

    def backlog_stats(self) -> BacklogStats:
        return self._repository.backlog_stats()

    # --- reads ---------------------------------------------------------------------------

    def get_credit(self, task_id: str) -> CreditOperation:
        operation = self._repository.get_operation(task_id)
        if operation is None:
            raise CreditNotFoundError(task_id)
        return operation

    def get_user(self, user_id: str) -> User:
        user = self._repository.get_user(user_id)
        if user is None:
            raise UserNotFoundError(user_id)
        return user
