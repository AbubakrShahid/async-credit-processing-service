import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from credit_service.errors import CreditNotFoundError, UserNotFoundError
from credit_service.models import ApplyOutcome, CreditOperation, User
from credit_service.repository import CreditRepository

logger = logging.getLogger(__name__)

type Enqueue = Callable[[str], None]


class CreditService:
    """Credit workflows shared by the API and the worker. No FastAPI or Celery in here."""

    def __init__(
        self, repository: CreditRepository, enqueue: Enqueue, max_attempts: int = 10
    ) -> None:
        self._repository = repository
        self._enqueue = enqueue
        self._max_attempts = max_attempts

    def submit_credit(
        self, user_id: str, amount: Decimal, idempotency_key: str | None = None
    ) -> CreditOperation:
        operation, created = self._repository.create_operation(user_id, amount, idempotency_key)
        if not created:
            return operation
        try:
            self._enqueue(operation.task_id)
        except Exception:
            # The operation is already durable in MongoDB, so the request is accepted
            # either way; the sweeper republishes it once it counts as stale.
            logger.exception("Failed to publish credit %s", operation.task_id)
        return operation

    def apply_credit(self, task_id: str) -> ApplyOutcome:
        """Apply a credit. Transient MongoDB errors propagate so the caller can retry."""
        attempts = self._repository.record_attempt(task_id)
        if attempts is not None and attempts > self._max_attempts:
            return self._fail(task_id, f"gave up after {self._max_attempts} attempts")

        try:
            outcome = self._repository.apply_credit(task_id)
        except UserNotFoundError as exc:
            # The user was deleted after the credit was accepted; retrying won't help.
            return self._fail(task_id, str(exc))

        if outcome is ApplyOutcome.NOT_FOUND:
            logger.warning("Credit %s does not exist; dropping task", task_id)
        return outcome

    def requeue_stale_credits(self, stale_after: timedelta, limit: int) -> int:
        """Republish credits stuck in `processing`. Returns how many were published.

        A requeued credit may still have its original message in the queue; that is
        harmless because applying a credit is idempotent.
        """
        task_ids = self._repository.claim_stale_operations(datetime.now(UTC) - stale_after, limit)
        for published, task_id in enumerate(task_ids):
            try:
                self._enqueue(task_id)
            except Exception:
                # Claimed but unpublished credits become stale again and are retried.
                logger.exception("Requeue failed with %d credits left", len(task_ids) - published)
                return published
        return len(task_ids)

    def _fail(self, task_id: str, reason: str) -> ApplyOutcome:
        logger.error("Credit %s failed: %s", task_id, reason)
        self._repository.fail_credit(task_id, reason)
        return ApplyOutcome.FAILED

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
