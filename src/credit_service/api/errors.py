from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from credit_service.errors import UserNotFoundError


async def user_not_found(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": str(exc)})


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(UserNotFoundError, user_not_found)
