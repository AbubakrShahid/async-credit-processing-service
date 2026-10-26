import hmac
from typing import Annotated

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

from credit_service.services import ANONYMOUS_CLIENT, CreditService

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_credit_service(request: Request) -> CreditService:
    service: CreditService = request.app.state.credit_service
    return service


def authenticate(request: Request, api_key: Annotated[str | None, Security(api_key_header)]) -> str:
    """Resolve the calling client from its API key. Disabled when no keys are configured."""
    clients: dict[str, str] = request.app.state.api_key_clients
    if not clients:
        return ANONYMOUS_CLIENT
    if api_key is not None:
        for key, client in clients.items():
            if hmac.compare_digest(key.encode(), api_key.encode()):
                return client
    raise HTTPException(
        status.HTTP_401_UNAUTHORIZED,
        "missing or invalid API key",
        headers={"WWW-Authenticate": "ApiKey"},
    )


CreditServiceDep = Annotated[CreditService, Depends(get_credit_service)]
ClientId = Annotated[str, Depends(authenticate)]
