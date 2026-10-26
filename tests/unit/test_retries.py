import pytest
from pymongo.errors import AutoReconnect, DuplicateKeyError, OperationFailure

from credit_service.db import is_transient
from credit_service.worker import retry_delay


@pytest.mark.parametrize("retries", range(10))
def test_retry_delay_is_jittered_exponential_and_capped(retries: int) -> None:
    delays = [retry_delay(retries, base=1.0, cap=60.0) for _ in range(200)]

    assert all(0 <= d <= min(60.0, 2**retries) for d in delays)
    assert len(set(delays)) > 1


@pytest.mark.parametrize(
    ("exc", "transient"),
    [
        (AutoReconnect("primary stepped down"), True),
        (
            OperationFailure(
                "write conflict", details={"errorLabels": ["TransientTransactionError"]}
            ),
            True,
        ),
        (DuplicateKeyError("dup"), False),
        (OperationFailure("bad query"), False),
        (ValueError("bug"), False),
    ],
)
def test_classifies_transient_mongo_errors(exc: Exception, transient: bool) -> None:
    assert is_transient(exc) is transient
