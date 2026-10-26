"""Many credits for the same user at once must add up to the exact total."""

import random
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.read_concern import ReadConcern

from credit_service.celery_app import enqueue_credit
from credit_service.db import Document
from credit_service.services import CreditService
from tests.integration.helpers import running_worker, wait_for


def random_amounts(count: int, seed: int) -> list[Decimal]:
    rng = random.Random(seed)
    return [Decimal(rng.randint(1, 100_000)) / 100 for _ in range(count)]


def assert_settled(db: Database[Document], user_id: str, expected_balance: Decimal) -> None:
    user = db.users.find_one({"user_id": user_id})
    assert user is not None
    assert user["balance"] == expected_balance
    assert (user["status"], user["pending_credits"]) == ("processed", 0)
    assert db.credit_operations.count_documents({"status": {"$ne": "processed"}}) == 0


def test_parallel_submits_and_applies_sum_exactly(
    service: CreditService, db: Database[Document], make_user: Callable[..., str]
) -> None:
    make_user("u_123", balance=Decimal("1000.00"))
    amounts = random_amounts(40, seed=7)

    def submit_and_apply(amount: Decimal) -> None:
        operation = service.submit_credit("u_123", amount)
        service.apply_credit(operation.task_id)

    # Submits and applies interleave, so both transactions contend on the same user doc.
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(submit_and_apply, amounts))

    assert_settled(db, "u_123", Decimal("1000.00") + sum(amounts))


def assert_invariants_hold(
    client: MongoClient[Document], db: Database[Document], user_id: str, initial: Decimal
) -> None:
    """Read user and credits from one snapshot, as they were at a single instant."""
    with (
        client.start_session() as session,
        session.start_transaction(read_concern=ReadConcern("snapshot")),
    ):
        user = db.users.find_one({"user_id": user_id}, session=session)
        operations = list(db.credit_operations.find({"user_id": user_id}, session=session))
    assert user is not None
    processed = sum((op["amount"] for op in operations if op["status"] == "processed"), Decimal(0))
    in_flight = sum(op["status"] == "processing" for op in operations)
    assert user.get("balance", Decimal(0)) == initial + processed
    assert user.get("pending_credits", 0) == in_flight


def test_concurrent_worker_applies_every_credit_exactly_once(
    make_service: Callable[..., CreditService],
    mongo_client: MongoClient[Document],
    db: Database[Document],
    make_user: Callable[..., str],
) -> None:
    service = make_service(enqueue_credit)
    make_user("u_123")
    amounts = random_amounts(30, seed=11)

    with running_worker(pool="threads", concurrency=8):
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda amount: service.submit_credit("u_123", amount), amounts))

        # Check the invariants repeatedly while credits are still being applied.
        def settled() -> bool:
            assert_invariants_hold(mongo_client, db, "u_123", Decimal(0))
            return db.credit_operations.count_documents({"status": "processed"}) == 30

        wait_for(settled)

    assert_settled(db, "u_123", sum(amounts, Decimal("0")))
