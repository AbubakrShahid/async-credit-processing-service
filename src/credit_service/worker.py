"""Celery worker entry point: `celery -A credit_service.worker worker`."""

import logging
import random
from datetime import timedelta
from functools import cache

from celery import Task

from credit_service.celery_app import (
    APPLY_CREDIT_TASK,
    REQUEUE_STALE_TASK,
    celery_app,
    enqueue_credit,
)
from credit_service.config import get_settings
from credit_service.db import create_client, get_database, is_transient
from credit_service.repository import CreditRepository
from credit_service.services import CreditService

logger = logging.getLogger(__name__)

app = celery_app


@cache
def get_service() -> CreditService:
    # Built lazily inside each worker process: MongoClient is not fork-safe, so it
    # must not be created in the parent before the prefork pool starts.
    settings = get_settings()
    client = create_client(settings)
    repository = CreditRepository(client, get_database(client, settings))
    return CreditService(repository, enqueue_credit, max_attempts=settings.max_attempts)


def retry_delay(retries: int, base: float, cap: float) -> float:
    """Exponential backoff with full jitter, so retrying workers don't stampede together."""
    return random.uniform(0, min(cap, base * 2**retries))


@celery_app.task(name=APPLY_CREDIT_TASK, bind=True, max_retries=get_settings().task_max_retries)
def apply_credit(self: "Task[[str], str]", task_id: str) -> str:
    """Apply one credit.

    Transient MongoDB errors are retried with backoff. If retries run out, the task fails
    but the credit stays `processing`, so it is picked up again later; the attempts cap
    in CreditService is what finally marks a credit as failed.
    """
    try:
        outcome = get_service().apply_credit(task_id)
    except Exception as exc:
        if not is_transient(exc):
            raise
        settings = get_settings()
        countdown = retry_delay(
            self.request.retries, settings.retry_backoff_seconds, settings.retry_backoff_max_seconds
        )
        logger.warning(
            "Transient error on credit %s, retrying in %.1fs: %s", task_id, countdown, exc
        )
        raise self.retry(exc=exc, countdown=countdown) from exc
    logger.info("Credit %s: %s", task_id, outcome)
    return outcome.value


@celery_app.task(name=REQUEUE_STALE_TASK)
def requeue_stale_credits() -> int:
    """Periodic (celery beat) safety net for credits whose message never got processed."""
    settings = get_settings()
    count = get_service().requeue_stale_credits(
        timedelta(seconds=settings.stale_after_seconds), settings.sweep_batch_size
    )
    if count:
        logger.warning("Requeued %d stale credits", count)
    return count
