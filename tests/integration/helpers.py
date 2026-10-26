import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from celery.contrib.testing.worker import start_worker

from credit_service.worker import celery_app


@contextmanager
def running_worker(pool: str = "solo", concurrency: int = 1) -> Iterator[None]:
    """A real Celery worker in this process, consuming the test queue from RabbitMQ."""
    celery_app.control.purge()
    # The worker's event loop only checks for shutdown when a timer fires; a short AMQP
    # heartbeat makes it stop in well under a second instead of ~10s.
    celery_app.conf.broker_heartbeat = 1
    with start_worker(
        celery_app,
        pool=pool,
        concurrency=concurrency,
        perform_ping_check=False,
        loglevel="WARNING",
    ):
        yield


def wait_for(predicate: Callable[[], bool], timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.1)
