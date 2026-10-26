"""Celery worker entry point: `celery -A credit_service.worker worker`."""

# celery-types makes Task generic for mypy only; keep annotations unevaluated at runtime.
from __future__ import annotations

import random
import time
from functools import cache

import structlog
from celery import Task
from celery.signals import setup_logging, task_prerun

from credit_service.celery_app import (
    APPLY_CREDIT_TASK,
    DISPATCH_TASK,
    QueueDepthProbe,
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
    repository = CreditRepository(client, get_database(client, settings), settings)
    queue_depth = QueueDepthProbe(celery_app, settings.credits_queue, ttl_seconds=0)
    return CreditService(repository, enqueue_credit, settings, queue_depth)


def retry_delay(retries: int, base: float, cap: float) -> float:
    """Exponential backoff with full jitter, so retrying workers don't stampede together."""
    return random.uniform(0, min(cap, base * 2**retries))


@celery_app.task(name=APPLY_CREDIT_TASK, bind=True, max_retries=get_settings().task_max_retries)
def apply_credit(self: Task[[str], str], task_id: str) -> str:
    """Apply one credit.

    Transient MongoDB errors are retried with backoff. If retries run out the task fails,
    but the credit stays `processing` and the dispatcher republishes it later: an outage
    delays a credit, it never fails it. Unexpected errors count towards the crash budget.
    """
    # RabbitMQ flags a message it delivers again after its consumer died without acking.
    redelivered = bool((self.request.delivery_info or {}).get("redelivered"))
    started = time.perf_counter()
    service = get_service()
    try:
        outcome = service.apply_credit(task_id, redelivered=redelivered)
    except Exception as exc:
        transient = is_transient(exc)
        service.record_failed_attempt(task_id, exc, transient=transient)
        if not transient:
            logger.exception("credit_processing_error")
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
        redelivered=redelivered,
        duration_ms=round((time.perf_counter() - started) * 1000, 1),
    )
    return outcome.value


@celery_app.task(name=DISPATCH_TASK)
def dispatch_pending_credits() -> int:
    """Periodic (celery beat): publish credits the API couldn't, recover lost messages."""
    service = get_service()
    report = service.dispatch_pending()
    stats = service.backlog_stats()
    log = logger.warning if report.published or not report.broker_available else logger.info
    log(
        "dispatch_run",
        published=report.published,
        reconciled=report.reconciled,
        broker_available=report.broker_available,
        unpublished=stats.unpublished,
        oldest_unpublished_age_s=round(stats.oldest_unpublished_age_seconds, 1),
        processing=stats.processing,
        oldest_processing_age_s=round(stats.oldest_processing_age_seconds, 1),
    )
    return report.published
