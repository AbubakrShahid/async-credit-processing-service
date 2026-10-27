"""Group commit: a busy account's pending credits are settled together, still each once."""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from pymongo.database import Database

from credit_service.db import Document
from credit_service.models import ApplyOutcome
from credit_service.repository import CreditRepository
from credit_service.services import CreditService


def user_of(db: Database[Document], user_id: str) -> Document:
    doc = db.users.find_one({"user_id": user_id})
    assert doc is not None
    return doc


def test_applying_one_credit_settles_the_users_other_pending_credits(
    service: CreditService, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123", balance=Decimal("100.00"))
    amounts = [Decimal("1.10"), Decimal("2.20"), Decimal("3.30"), Decimal("4.40")]
    task_ids = [service.submit_credit("u_123", amount).task_id for amount in amounts]

    assert service.apply_credit(task_ids[0]) is ApplyOutcome.APPLIED

    settled = list(db.credit_operations.find({"_id": {"$in": task_ids}}))
    assert {(c["status"], c["settled_by"]) for c in settled} == {("processed", task_ids[0])}
    user = user_of(db, "u_123")
    assert (user["balance"], user["pending_credits"], user["status"]) == (
        Decimal("111.00"),
        0,
        "processed",
    )
    # Their own messages arrive later and change nothing.
    for task_id in task_ids[1:]:
        assert service.apply_credit(task_id) is ApplyOutcome.ALREADY_SETTLED
    assert user_of(db, "u_123")["balance"] == Decimal("111.00")


def test_batch_is_bounded_and_takes_the_oldest_credits_first(
    make_service: Callable[..., CreditService],
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    service = make_service(apply_batch_size=3)
    make_user("u_123")
    task_ids = [service.submit_credit("u_123", Decimal("1")).task_id for _ in range(5)]

    service.apply_credit(task_ids[4])  # the newest triggers; the two oldest join it

    processed = {c["_id"] for c in db.credit_operations.find({"status": "processed"})}
    assert processed == {task_ids[4], task_ids[0], task_ids[1]}
    user = user_of(db, "u_123")
    assert (user["balance"], user["pending_credits"], user["status"]) == (
        Decimal("3.00"),
        2,
        "processing",
    )


def test_batch_never_touches_other_users_or_parked_credits(
    service: CreditService,
    repository: CreditRepository,
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    make_user("u_123")
    make_user("u_other")
    mine = service.submit_credit("u_123", Decimal("1")).task_id
    parked = service.submit_credit("u_123", Decimal("50")).task_id
    other = service.submit_credit("u_other", Decimal("7")).task_id
    repository.park_credit(parked, "under review")

    service.apply_credit(mine)

    statuses = {c["_id"]: c["status"] for c in db.credit_operations.find()}
    assert statuses == {mine: "processed", parked: "processing", other: "processing"}
    user = user_of(db, "u_123")
    assert (user["balance"], user["pending_credits"]) == (Decimal("1.00"), 1)  # parked still owed


def test_concurrent_batches_for_one_account_settle_every_credit_exactly_once(
    service: CreditService, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123", balance=Decimal("0.00"))
    amounts = [Decimal(n) / 100 for n in range(1, 81)]
    task_ids = [service.submit_credit("u_123", amount).task_id for amount in amounts]

    # Every credit's message is processed concurrently, as 8 workers would.
    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(service.apply_credit, task_ids))

    assert set(outcomes) <= {ApplyOutcome.APPLIED, ApplyOutcome.ALREADY_SETTLED}
    assert db.credit_operations.count_documents({"status": "processed"}) == 80
    user = user_of(db, "u_123")
    assert (user["balance"], user["pending_credits"], user["status"]) == (
        sum(amounts),
        0,
        "processed",
    )


def test_one_corrupt_credit_cannot_block_the_rest_of_the_account(
    service: CreditService, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123")
    corrupt = service.submit_credit("u_123", Decimal("5")).task_id
    good = service.submit_credit("u_123", Decimal("10")).task_id
    db.credit_operations.update_one({"_id": corrupt}, {"$set": {"amount": "not money"}})

    assert service.apply_credit(good) is ApplyOutcome.APPLIED  # batch fails, falls back

    assert db.credit_operations.find_one({"_id": good})["status"] == "processed"
    assert db.credit_operations.find_one({"_id": corrupt})["status"] == "processing"
    assert user_of(db, "u_123")["balance"] == Decimal("10.00")
