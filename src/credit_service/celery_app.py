from celery import Celery

from credit_service.config import Settings, get_settings

APPLY_CREDIT_TASK = "credit_service.apply_credit"
CREDITS_QUEUE = "credits"


def create_celery(settings: Settings) -> Celery:
    app = Celery("credit_service", broker=settings.broker_url)
    app.conf.update(
        task_default_queue=CREDITS_QUEUE,
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
    )
    return app


celery_app = create_celery(get_settings())


def enqueue_credit(task_id: str) -> None:
    """Publish by task name so the API does not import worker code.

    The message carries only the operation id; the worker reads everything else from
    MongoDB. The Celery task id is the operation id, so both refer to the same thing.
    """
    celery_app.send_task(APPLY_CREDIT_TASK, args=[task_id], task_id=task_id)
