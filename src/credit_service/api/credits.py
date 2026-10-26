from typing import Annotated

from fastapi import APIRouter, Depends, Header, Request, Response, status

from credit_service.api.dependencies import ClientId, CreditServiceDep, authenticate, db_deadline
from credit_service.api.schemas import CreditAccepted, CreditDetails, CreditRequest

router = APIRouter(prefix="/credits", tags=["credits"], dependencies=[Depends(authenticate)])


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "User does not exist"},
        status.HTTP_409_CONFLICT: {"description": "Idempotency key reused with a different body"},
        status.HTTP_429_TOO_MANY_REQUESTS: {"description": "Too many credits in flight for user"},
        status.HTTP_503_SERVICE_UNAVAILABLE: {"description": "Backlogged or database unavailable"},
    },
)
def submit_credit(
    body: CreditRequest,
    request: Request,
    response: Response,
    service: CreditServiceDep,
    client_id: ClientId,
    idempotency_key: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=255,
            description="Retrying with the same key returns the original credit. "
            "Keys are scoped to the calling client.",
        ),
    ] = None,
) -> CreditAccepted:
    """Accept a credit and apply it asynchronously.

    A sync handler on purpose: MongoDB and Celery publishing are blocking calls, so
    FastAPI runs this in its threadpool instead of stalling the event loop.
    """
    with db_deadline(request):
        operation = service.submit_credit(body.user_id, body.amount, idempotency_key, client_id)
    status_url = f"/credits/{operation.task_id}"
    response.headers["Location"] = status_url
    return CreditAccepted(task_id=operation.task_id, status=operation.status, status_url=status_url)


@router.get("/{task_id}", responses={status.HTTP_404_NOT_FOUND: {"description": "Unknown task id"}})
def get_credit(task_id: str, request: Request, service: CreditServiceDep) -> CreditDetails:
    """Status of a credit; `processed` means the balance has been updated."""
    with db_deadline(request):
        return CreditDetails.from_operation(service.get_credit(task_id))
