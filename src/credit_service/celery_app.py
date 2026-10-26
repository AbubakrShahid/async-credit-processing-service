import threading
import time
from typing import Any

from celery import Celery

from credit_service.config import Settings, get_settings

APPLY_CREDIT_TASK = "credit_service.apply_credit"
DISPATCH_TASK = "credit_service.dispatch_pending_credits"


def create_celery(settings: Settings) -> Celery:
    app = Celery("credit_service", broker=settings.broker_url)
    app.conf.update(
        task_default_queue=settings.credits_queue,
        # At-least-once: ack only after the task finished, and requeue if the worker
        # process dies mid-task. Safe because applying a credit is idempotent; repeated
        # crashes are bounded by the crash budget in CreditService.
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        worker_prefetch_multiplier=1,
        # A hung task is killed (and counted as a crash) instead of blocking a worker.
        task_time_limit=settings.task_time_limit_seconds,
        task_serializer="json",
        accept_content=["json"],
        # Status lives in MongoDB, not in a Celery result backend.
        task_ignore_result=True,
        # Wait for RabbitMQ to confirm each publish instead of fire-and-forget.
        broker_transport_options={"confirm_publish": True},
        broker_connection_timeout=2,
        broker_connection_retry_on_startup=True,
        task_publish_retry_policy={
            "max_retries": 2,
            "interval_start": 0,
            "interval_step": 0.5,
            "interval_max": 1,
        },
        beat_schedule={
            "dispatch-pending-credits": {
                "task": DISPATCH_TASK,
                "schedule": settings.dispatch_interval_seconds,
                # Don't let runs pile up while no worker is consuming.
                "options": {"expires": settings.dispatch_interval_seconds},
            }
        },
    )
    return app


celery_app = create_celery(get_settings())


def enqueue_credit(task_id: str) -> None:
    """Publish by task name so the API does not import worker code.

    The message carries only the operation id; the worker reads everything else from
    MongoDB. The Celery task id is the operation id, so both refer to the same thing.
    """
    celery_app.send_task(APPLY_CREDIT_TASK, args=[task_id], task_id=task_id)


class QueueDepthProbe:
    """Number of ready messages in the credits queue; None when it can't be determined.

    Cached for `ttl_seconds` so the API doesn't ask RabbitMQ on every request. Only one
    thread refreshes at a time; the others keep using the cached value meanwhile.
    """

    def __init__(self, app: Celery, queue: str, ttl_seconds: float) -> None:
        self._app = app
        self._queue = queue
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
        self._value: int | None = None
        self._expires = 0.0

    def __call__(self) -> int | None:
        if time.monotonic() < self._expires or not self._lock.acquire(blocking=False):
            return self._value
        try:
            self._value = self._read()
            self._expires = time.monotonic() + self._ttl
        finally:
            self._lock.release()
        return self._value

    def _read(self) -> int | None:
        try:
            with self._app.connection_for_write(connect_timeout=1) as connection:
                channel: Any = connection.channel()  # kombu stubs lack queue_declare
                try:
                    declared = channel.queue_declare(queue=self._queue, passive=True)
                finally:
                    channel.close()
        except Exception:
            return None
        return int(declared.message_count)
