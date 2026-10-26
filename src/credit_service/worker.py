"""Celery worker entry point: `celery -A credit_service.worker worker`."""

# celery-types makes Task generic for mypy only; keep annotations unevaluated at runtime.
from __future__ import annotations

import random
import time
from datetime import timedelta
from functools import cache

import structlog
from celery import Task
from celery.signals import setup_logging, task_prerun

from credit_service.celery_app import (
    APPLY_CREDIT_TASK,
    REQUEUE_STALE_TASK,
    celery_app,
    enqueue_credit,
)
from credit_service.config import get_settings
from credit_service.db import create_client, get_database, is_transient
from credit_service.observability import configure_logging
from credit_service.repository import CreditRepository
from credit_service.services import CreditService

logger = structlog.get_logger(__name__)


@setup_logging.connect
def _configure_logging(**_: object) -> None:
    # Connecting this signal stops Celery from installing its own log handlers.
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)


@task_prerun.connect
def _bind_task_context(task_id: str, task: Task[..., object], **_: object) -> None:
    # For credit tasks the Celery task id is the credit id, so logs join up with the API's.
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(task_id=task_id, task=task.name)


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
def apply_credit(self: Task[[str], str], task_id: str) -> str:
    """Apply one credit.

    Transient MongoDB errors are retried with backoff. If retries run out, the task fails
    but the credit stays `processing`, so it is picked up again later; the attempts cap
    in CreditService is what finally marks a credit as failed.
    """
    started = time.perf_counter()
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
            "credit_retry_scheduled",
            retry=self.request.retries + 1,
            countdown_s=round(countdown, 2),
            error=repr(exc),
        )
        raise self.retry(exc=exc, countdown=countdown) from exc
    logger.info(
        "credit_processed",
        outcome=outcome.value,
        duration_ms=round((time.perf_counter() - started) * 1000, 1),
    )
    return outcome.value


@celery_app.task(name=REQUEUE_STALE_TASK)
def requeue_stale_credits() -> int:
    """Periodic (celery beat) safety net for credits whose message never got processed."""
    settings = get_settings()
    count = get_service().requeue_stale_credits(
        timedelta(seconds=settings.stale_after_seconds), settings.sweep_batch_size
    )
    if count:
        logger.warning("stale_credits_requeued", count=count)
    return count
