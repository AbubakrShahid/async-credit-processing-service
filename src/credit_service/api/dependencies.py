from typing import Annotated

from fastapi import Depends, Request

from credit_service.services import CreditService


def get_credit_service(request: Request) -> CreditService:
    service: CreditService = request.app.state.credit_service
    return service


CreditServiceDep = Annotated[CreditService, Depends(get_credit_service)]
