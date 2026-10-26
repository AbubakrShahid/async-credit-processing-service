"""Prometheus metrics.

Backlog gauges are read from MongoDB at scrape time, so they are correct no matter how many
API and worker processes run; they are what alerts should key on (credits stuck or ageing,
failures, queue depth). HTTP metrics are per API process.
"""

import time
from collections.abc import Awaitable, Callable, Iterable

from fastapi import APIRouter, FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Histogram, generate_latest
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector
from pymongo.errors import PyMongoError

from credit_service.services import CreditService, QueueDepth

router = APIRouter()


class BacklogCollector(Collector):
    def __init__(self, service: CreditService, queue_depth: QueueDepth) -> None:
        self._service = service
        self._queue_depth = queue_depth

    def collect(self) -> Iterable[Metric]:
        try:
            stats = self._service.backlog_stats()
        except PyMongoError:
            yield GaugeMetricFamily("credit_backlog_up", "Backlog metrics readable", value=0)
            return
        yield GaugeMetricFamily("credit_backlog_up", "Backlog metrics readable", value=1)
        gauges = {
            "credit_processing": ("Credits accepted but not settled", stats.processing),
            "credit_unpublished": ("Credits not yet confirmed by the broker", stats.unpublished),
            "credit_failed": ("Credits that failed permanently", stats.failed),
            "credit_oldest_processing_age_seconds": (
                "Age of the oldest unsettled credit",
                stats.oldest_processing_age_seconds,
            ),
            "credit_oldest_unpublished_age_seconds": (
                "Age of the oldest credit not yet published",
                stats.oldest_unpublished_age_seconds,
            ),
        }
        for name, (description, value) in gauges.items():
            yield GaugeMetricFamily(name, description, value=value)
        depth = self._queue_depth()
        if depth is not None:
            yield GaugeMetricFamily(
                "credit_queue_depth", "Ready messages in the queue", value=depth
            )


def instrument(app: FastAPI, registry: CollectorRegistry) -> None:
    latency = Histogram(
        "http_request_duration_seconds",
        "HTTP request latency",
        ["method", "route", "status"],
        registry=registry,
    )

    @app.middleware("http")
    async def record_latency(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        started = time.perf_counter()
        response = await call_next(request)
        route = request.scope.get("route")
        latency.labels(
            request.method, getattr(route, "path", "unmatched"), response.status_code
        ).observe(time.perf_counter() - started)
        return response


@router.get("/metrics", include_in_schema=False)
def metrics(request: Request) -> Response:
    registry: CollectorRegistry = request.app.state.metrics_registry
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
