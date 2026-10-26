from fastapi import APIRouter, Response, status

from credit_service.api.dependencies import CreditServiceDep
from credit_service.api.schemas import CreditAccepted, CreditRequest

router = APIRouter(prefix="/credits", tags=["credits"])


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={status.HTTP_404_NOT_FOUND: {"description": "User does not exist"}},
)
def submit_credit(
    body: CreditRequest, response: Response, service: CreditServiceDep
) -> CreditAccepted:
    """Accept a credit and apply it asynchronously.

    A sync handler on purpose: MongoDB and Celery publishing are blocking calls, so
    FastAPI runs this in its threadpool instead of stalling the event loop.
    """
    operation = service.submit_credit(body.user_id, body.amount)
    status_url = f"/credits/{operation.task_id}"
    response.headers["Location"] = status_url
    return CreditAccepted(task_id=operation.task_id, status=operation.status, status_url=status_url)
