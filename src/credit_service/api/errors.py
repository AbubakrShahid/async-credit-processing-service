from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError

from credit_service.db import is_transient
from credit_service.errors import (
    CreditNotFoundError,
    IdempotencyConflictError,
    ServiceOverloadedError,
    TooManyPendingCreditsError,
    UserNotFoundError,
)

type ExceptionHandler = Callable[[Request, Exception], Awaitable[JSONResponse]]


def _respond_with(status_code: int, retry_after: int | None = None) -> ExceptionHandler:
    headers = {"Retry-After": str(retry_after)} if retry_after is not None else None

    async def handle(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(status_code=status_code, content={"detail": str(exc)}, headers=headers)

    return handle


async def mongo_error(_: Request, exc: Exception) -> JSONResponse:
    if not is_transient(exc):
        raise exc
    # The request may or may not have been committed (e.g. a lost commit reply), so the
    # safe instruction is to retry with the same idempotency key.
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "database temporarily unavailable; retry with the same Idempotency-Key"},
        headers={"Retry-After": "1"},
    )


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(UserNotFoundError, _respond_with(status.HTTP_404_NOT_FOUND))
    app.add_exception_handler(CreditNotFoundError, _respond_with(status.HTTP_404_NOT_FOUND))
    app.add_exception_handler(IdempotencyConflictError, _respond_with(status.HTTP_409_CONFLICT))
    app.add_exception_handler(
        TooManyPendingCreditsError, _respond_with(status.HTTP_429_TOO_MANY_REQUESTS, 5)
    )
    app.add_exception_handler(
        ServiceOverloadedError, _respond_with(status.HTTP_503_SERVICE_UNAVAILABLE, 5)
    )
    app.add_exception_handler(PyMongoError, mongo_error)
