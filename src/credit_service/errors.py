class CreditServiceError(Exception):
    pass


class UserNotFoundError(CreditServiceError):
    def __init__(self, user_id: str) -> None:
        super().__init__(f"user '{user_id}' not found")
        self.user_id = user_id


class IdempotencyConflictError(CreditServiceError):
    def __init__(self, key: str) -> None:
        super().__init__(f"idempotency key '{key}' was already used with a different request")
        self.key = key


class CreditNotFoundError(CreditServiceError):
    def __init__(self, task_id: str) -> None:
        super().__init__(f"credit '{task_id}' not found")
        self.task_id = task_id
