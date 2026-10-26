"""Celery worker entry point: `celery -A credit_service.worker worker`."""

import logging
from functools import cache

from credit_service.celery_app import APPLY_CREDIT_TASK, celery_app, enqueue_credit
from credit_service.config import get_settings
from credit_service.db import create_client, get_database
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
    return CreditService(CreditRepository(client, get_database(client, settings)), enqueue_credit)


@celery_app.task(name=APPLY_CREDIT_TASK)
def apply_credit(task_id: str) -> str:
    outcome = get_service().apply_credit(task_id)
    logger.info("Credit %s: %s", task_id, outcome)
    return outcome.value
