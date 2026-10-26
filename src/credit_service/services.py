import logging
from collections.abc import Callable
from decimal import Decimal

from credit_service.models import ApplyOutcome, CreditOperation
from credit_service.repository import CreditRepository

logger = logging.getLogger(__name__)

type Enqueue = Callable[[str], None]


class CreditService:
    """Credit workflows shared by the API and the worker. No FastAPI or Celery in here."""

    def __init__(self, repository: CreditRepository, enqueue: Enqueue) -> None:
        self._repository = repository
        self._enqueue = enqueue

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
            # either way; it stays in `processing` until it is published again.
            logger.exception("Failed to publish credit %s", operation.task_id)
        return operation

    def apply_credit(self, task_id: str) -> ApplyOutcome:
        outcome = self._repository.apply_credit(task_id)
        if outcome is ApplyOutcome.NOT_FOUND:
            logger.warning("Credit %s does not exist; dropping task", task_id)
        return outcome
