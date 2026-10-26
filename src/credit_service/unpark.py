"""Operator command: `python -m credit_service.unpark <task_id> [...]`.

Releases credits parked after repeated crashes, once the cause has been understood. The
dispatcher republishes them on its next run, with a fresh crash budget.
"""

import sys

from credit_service.celery_app import enqueue_credit
from credit_service.config import get_settings
from credit_service.db import create_client, get_database
from credit_service.repository import CreditRepository
from credit_service.services import CreditService


def main(task_ids: list[str]) -> int:
    settings = get_settings()
    client = create_client(settings)
    service = CreditService(
        CreditRepository(client, get_database(client, settings), settings), enqueue_credit, settings
    )
    missing = [task_id for task_id in task_ids if not service.unpark_credit(task_id)]
    for task_id in task_ids:
        print(f"{task_id}: {'not parked' if task_id in missing else 'released'}")
    client.close()
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
