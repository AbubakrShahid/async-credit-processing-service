class CreditServiceError(Exception):
    pass


class UserNotFoundError(CreditServiceError):
    def __init__(self, user_id: str) -> None:
        super().__init__(f"user '{user_id}' not found")
        self.user_id = user_id
