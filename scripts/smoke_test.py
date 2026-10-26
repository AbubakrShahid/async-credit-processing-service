"""End-to-end smoke test against the running stack (`make up && make seed` first).

Standard library only, so it runs with any Python 3.
"""

import json
import os
import sys
import time
import urllib.request
import uuid
from decimal import Decimal

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
API_KEY = os.environ.get("API_KEY", "local-dev-key")


def call(method: str, path: str, body: dict[str, str] | None = None) -> tuple[int, dict]:
    request = urllib.request.Request(
        BASE_URL + path,
        method=method,
        data=json.dumps(body).encode() if body else None,
        headers={
            "Content-Type": "application/json",
            "Idempotency-Key": str(uuid.uuid4()),
            "X-API-Key": API_KEY,
        },
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return response.status, json.load(response)


def main() -> None:
    _, before = call("GET", "/users/u_123")
    status, accepted = call("POST", "/credits", {"user_id": "u_123", "amount": "50.00"})
    assert status == 202, status
    print(f"accepted {accepted['task_id']}")

    deadline = time.monotonic() + 30
    while True:
        _, credit = call("GET", accepted["status_url"])
        if credit["status"] != "processing":
            break
        if time.monotonic() > deadline:
            sys.exit("credit still processing after 30s")
        time.sleep(0.5)

    _, after = call("GET", "/users/u_123")
    assert credit["status"] == "processed", credit
    assert Decimal(after["balance"]) - Decimal(before["balance"]) == Decimal("50.00"), after
    print(f"processed: balance {before['balance']} -> {after['balance']}")


if __name__ == "__main__":
    main()
