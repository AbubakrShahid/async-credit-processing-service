from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_client import CollectorRegistry

from credit_service.api import credits, health, metrics, users
from credit_service.api.errors import register_error_handlers
from credit_service.celery_app import QueueDepthProbe, celery_app, enqueue_credit
from credit_service.config import get_settings
from credit_service.db import create_client, ensure_indexes, get_database
from credit_service.observability import configure_logging
from credit_service.repository import CreditRepository
from credit_service.services import CreditService


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    client = create_client(settings)
    db = get_database(client, settings)
    ensure_indexes(db)
    queue_depth = QueueDepthProbe(celery_app, settings.credits_queue, ttl_seconds=1.0)
    service = CreditService(
        CreditRepository(client, db, settings), enqueue_credit, settings, queue_depth
    )
    collector = metrics.BacklogCollector(service, queue_depth)

    app.state.mongo_client = client
    app.state.credit_service = service
    app.state.api_key_clients = settings.api_key_clients()
    app.state.metrics_registry.register(collector)
    yield
    app.state.metrics_registry.unregister(collector)
    client.close()


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    app = FastAPI(
        title="Credit Service",
        description="Credits user balances asynchronously via RabbitMQ and Celery.",
        version="0.2.0",
        lifespan=lifespan,
    )
    app.state.metrics_registry = CollectorRegistry()
    metrics.instrument(app, app.state.metrics_registry)
    app.include_router(credits.router)
    app.include_router(users.router)
    app.include_router(health.router)
    app.include_router(metrics.router)
    register_error_handlers(app)
    return app


app = create_app()
