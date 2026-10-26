from fastapi import APIRouter, status

from credit_service.api.dependencies import CreditServiceDep
from credit_service.api.schemas import UserBalance

router = APIRouter(prefix="/users", tags=["users"])


@router.get(
    "/{user_id}", responses={status.HTTP_404_NOT_FOUND: {"description": "User does not exist"}}
)
def get_user(user_id: str, service: CreditServiceDep) -> UserBalance:
    return UserBalance.from_user(service.get_user(user_id))
