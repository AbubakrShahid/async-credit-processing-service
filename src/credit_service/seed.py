"""Create demo users: `python -m credit_service.seed`. Safe to run repeatedly."""

from decimal import Decimal

from credit_service.config import get_settings
from credit_service.db import Document, create_client, ensure_indexes, get_database

DEMO_USERS: list[Document] = [
    # Matches the example in the brief.
    {"user_id": "u_123", "balance": Decimal("100.00"), "status": "processed", "pending_credits": 0},
    # No balance field yet: the first credit creates it.
    {"user_id": "u_new"},
    {"user_id": "u_alice", "balance": Decimal("0.00"), "status": "processed", "pending_credits": 0},
]


def main() -> None:
    settings = get_settings()
    client = create_client(settings)
    db = get_database(client, settings)
    ensure_indexes(db)
    for user in DEMO_USERS:
        result = db.users.update_one(
            {"user_id": user["user_id"]}, {"$setOnInsert": user}, upsert=True
        )
        print(f"{user['user_id']}: {'created' if result.upserted_id else 'already exists'}")
    client.close()


if __name__ == "__main__":
    main()
