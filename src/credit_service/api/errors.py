from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from credit_service.errors import IdempotencyConflictError, UserNotFoundError

type ExceptionHandler = Callable[[Request, Exception], Awaitable[JSONResponse]]


def _respond_with(status_code: int) -> ExceptionHandler:
    async def handle(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(status_code=status_code, content={"detail": str(exc)})

    return handle


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(UserNotFoundError, _respond_with(status.HTTP_404_NOT_FOUND))
    app.add_exception_handler(IdempotencyConflictError, _respond_with(status.HTTP_409_CONFLICT))
