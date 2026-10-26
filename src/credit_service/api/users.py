from fastapi import APIRouter, Depends, Request, status

from credit_service.api.dependencies import CreditServiceDep, authenticate, db_deadline
from credit_service.api.schemas import UserBalance

router = APIRouter(prefix="/users", tags=["users"], dependencies=[Depends(authenticate)])


@router.get(
    "/{user_id}", responses={status.HTTP_404_NOT_FOUND: {"description": "User does not exist"}}
)
def get_user(user_id: str, request: Request, service: CreditServiceDep) -> UserBalance:
    with db_deadline(request):
        return UserBalance.from_user(service.get_user(user_id))
