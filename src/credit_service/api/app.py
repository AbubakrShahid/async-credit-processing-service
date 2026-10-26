from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from credit_service.api import credits
from credit_service.api.errors import register_error_handlers
from credit_service.celery_app import enqueue_credit
from credit_service.config import get_settings
from credit_service.db import create_client, ensure_indexes, get_database
from credit_service.repository import CreditRepository
from credit_service.services import CreditService


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    client = create_client(settings)
    db = get_database(client, settings)
    ensure_indexes(db)
    app.state.credit_service = CreditService(CreditRepository(client, db), enqueue_credit)
    yield
    client.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Credit Service",
        description="Credits user balances asynchronously via RabbitMQ and Celery.",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(credits.router)
    register_error_handlers(app)
    return app


app = create_app()
