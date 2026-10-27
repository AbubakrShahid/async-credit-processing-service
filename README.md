# Async Credit Processing Service

A reliable asynchronous credit-processing service built with **FastAPI, Celery, RabbitMQ, and MongoDB**.

The API accepts a credit request, saves it, and returns `202 Accepted` without waiting for the balance update. A Celery worker processes the credit in the background, while MongoDB tracks its status.

The main engineering challenge is ensuring that **each credit changes the balance only once**, even when requests are retried, messages are delivered multiple times, or infrastructure fails.

**Tech Stack:** Python 3.12 · FastAPI · Celery · RabbitMQ · MongoDB 8.2 · Docker Compose

**[Two-Page Architecture & Design Document](docs/design.pdf)**

## Architecture

```text
                         Client
                           |
                     POST /credits
                           |
                           v
                        FastAPI
                           |
                +----------+----------+
                |                     |
                v                     v
             MongoDB               RabbitMQ
         Save credit request       Task queue
                ^                     |
                |                     v
                |                 Celery Worker
                |                     |
                +---------------------+
                 Update balance and
                 mark credit processed

         Celery Beat (every 15s)
                   |
                   v
               Dispatcher
                   |
          Recover pending tasks
```

### How it works

1. **Accept:** FastAPI validates the request and saves a credit operation with `processing` status in MongoDB.
2. **Publish:** The API publishes the credit ID to RabbitMQ and returns `202 Accepted`. If publishing fails, the credit remains saved for later recovery.
3. **Process:** A Celery worker reads the credit and updates the user's balance and operation status in one MongoDB transaction.
4. **Acknowledge:** The worker acknowledges the RabbitMQ message only after the transaction commits.
5. **Recover:** A scheduled dispatcher republishes credits that were never confirmed by RabbitMQ or may require recovery.

MongoDB is the source of truth. RabbitMQ handles message delivery, and Celery executes background tasks.

Each credit has its own operation record, allowing multiple credits for the same user to be tracked independently.

## Quick Start

### Prerequisites

- Docker with Docker Compose v2
- [uv](https://docs.astral.sh/uv/) for running tests locally

No MongoDB Atlas account or cloud services are required.

### Run the application

```bash
make up       # Build and start all services
make seed     # Create demo users
make smoke    # Verify credit processing end to end
```

The Docker environment starts FastAPI, MongoDB, RabbitMQ, a Celery worker, and Celery Beat.

### Available services

| Service | URL / Connection |
|---|---|
| Swagger UI | http://localhost:8000/docs |
| RabbitMQ Management | http://localhost:15672 |
| Prometheus Metrics | http://localhost:8000/metrics |
| MongoDB | `mongodb://localhost:27017/?replicaSet=rs0&directConnection=true` |

**Demo credentials:**

- API key: `local-dev-key`
- RabbitMQ: `guest / guest`

Open Swagger UI, click **Authorize**, and enter the API key to try the endpoints.

### Useful commands

```bash
make logs           # Follow service logs
make smoke          # Run the end-to-end smoke test
make check          # Lint, type-check, and run all tests
make alerts-check   # Validate and test alert rules
make down           # Stop services and DELETE data volumes
```

> **Note:** `make down` removes the local data volumes. Use it only when you want to reset the environment.

## API Usage

All credit and user endpoints require the `X-API-Key` header.

### 1. Submit a credit

```bash
curl -X POST http://localhost:8000/credits \
  -H "X-API-Key: local-dev-key" \
  -H "Idempotency-Key: refund-001" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"u_123","amount":"50.00"}'
```

Response (`202 Accepted`):

```json
{
  "task_id": "cr_abc123",
  "status": "processing",
  "status_url": "/credits/cr_abc123"
}
```

The request is saved before the response is returned. The balance update happens asynchronously.

The optional `Idempotency-Key` allows clients to retry safely. Repeating a request with the same key and body returns the original credit instead of creating another one.

### 2. Check credit status

```bash
curl http://localhost:8000/credits/cr_abc123 \
  -H "X-API-Key: local-dev-key"
```

Example response:

```json
{
  "task_id": "cr_abc123",
  "user_id": "u_123",
  "amount": "50.00",
  "status": "processed",
  "attempts": 1,
  "error": null,
  "created_at": "2026-10-26T12:37:38Z",
  "processed_at": "2026-10-26T12:37:39Z",
  "published_at": "2026-10-26T12:37:38Z",
  "parked_at": null
}
```

A credit can be `processing`, `processed`, or `failed`. A credit placed on hold for manual investigation remains `processing` and has a `parked_at` timestamp.

### 3. Check user balance

```bash
curl http://localhost:8000/users/u_123 \
  -H "X-API-Key: local-dev-key"
```

Example response after processing:

```json
{
  "user_id": "u_123",
  "balance": "150.00",
  "status": "processed",
  "pending_credits": 0
}
```

Money is returned as a decimal string to preserve precision.

### API responses

| Status | Meaning |
|---|---|
| `202` | Credit accepted for asynchronous processing |
| `401` | Missing or invalid API key |
| `404` | User or credit not found |
| `409` | Idempotency key reused with different request data |
| `422` | Invalid credit amount |
| `429` | Too many pending credits for the user |
| `503` | Service temporarily unavailable or overloaded |

Clients should reuse their original idempotency key when retrying an uncertain request.

## Reliability & Design Decisions

The service uses several safeguards to protect financial correctness and recover from failures.

| Concern | Implementation |
|---|---|
| Duplicate message delivery | Conditional status transition and balance update in one MongoDB transaction |
| Client retries | Per-client idempotency keys and unique database indexes |
| Worker crashes | Late RabbitMQ acknowledgments and idempotent processing |
| Broker outages | Database-backed recovery dispatcher |
| Concurrent credits | MongoDB transactions and atomic balance updates |
| Monetary precision | Python `Decimal` and MongoDB `Decimal128` |
| Transient infrastructure failures | Retries with exponential backoff and jitter |
| Repeated worker crashes | Park the credit for operator investigation |
| Excessive workload | Per-user pending limits and queue backlog protection |
| Data durability | Explicit majority-journaled MongoDB writes and durable RabbitMQ messages |

### Exactly-once balance effects

RabbitMQ provides at-least-once delivery, meaning a task may be delivered more than once.

The worker prevents duplicate financial effects by checking the credit's current status before updating the balance. Both changes commit in the same MongoDB transaction.

If a worker commits a credit but crashes before acknowledging RabbitMQ, the message may be redelivered. The next worker sees that the credit is already processed and skips the balance update.

**Messages may be processed more than once, but each credit can change the balance only once.**

### Recovery dispatcher

Celery Beat schedules the dispatcher every 15 seconds.

The dispatcher handles:

- **Unpublished credits:** Operations saved in MongoDB but never confirmed by RabbitMQ.
- **Potentially lost messages:** Older, confirmed operations that remain unprocessed and may need redelivery.

It uses atomic claims, leases, and bounded batches to coordinate concurrent dispatchers. Previously confirmed credits are only republished when the queue backlog is sufficiently low, avoiding unnecessary duplicate messages during congestion.

The credit operation also serves as the durable outbox record, so a separate outbox collection is not required.

### Group commit

When many credits target the same user, separate transactions repeatedly update the same user document, creating write contention.

The worker can settle up to **100 pending credits for one user in a single transaction**. Each operation retains its own status and audit information, while the user's balance is updated once with the combined amount.

This improves throughput for busy accounts while preserving individual credit tracking and duplicate protection.

### Failure handling

Failures are handled according to their cause:

- **Infrastructure failures:** Retry and recover without marking valid credits as failed.
- **Permanent business failures:** Mark the credit as `failed` when processing is no longer valid, such as when its user has been deleted.
- **Repeated crashes:** Park the credit rather than discarding an accepted financial obligation.

Parked credits remain outstanding and require operator investigation.

Once the problem is resolved, an operator can release a credit using:

```bash
docker compose exec api python -m credit_service.unpark <task_id>
```

The dispatcher then makes it eligible for processing again.

## Testing

The project includes **94 automated tests**, covering business rules, concurrency, message delivery, failure recovery, and the complete processing flow.

Integration tests use real MongoDB and RabbitMQ instances rather than mocking their transaction and message-delivery behavior.

### Run tests

```bash
uv sync
make up
make check
```

Other commands:

```bash
make test-unit      # Unit tests only
make alerts-check   # Test Prometheus alert rules
make smoke          # Verify the complete credit flow
```

### Key scenarios covered

| Scenario | Expected behavior |
|---|---|
| Valid credit | Balance updated, status processed |
| Duplicate API request | Same credit returned; no additional balance change |
| Duplicate RabbitMQ delivery | Balance updated only once |
| Worker crashes after commit | Redelivery does not duplicate the credit |
| MongoDB commit response lost | Safe recovery without a second balance update |
| RabbitMQ unavailable | Saved credit is dispatched after recovery |
| MongoDB temporarily unavailable | Credit remains pending and is retried |
| Concurrent credits | Exact balance and pending count |
| Repeated worker crashes | Credit parked for investigation |
| Group commit | Multiple credits settled together without double processing |
| Excessive requests | `429` or `503` before accepting new work |

The tests also verify accounting invariants during concurrent processing:

```text
Current balance = Opening balance + Sum of processed credits

Pending credits = Number of unsettled credit operations
```

### Failure-recovery verification

The following scenarios were also tested on the local Docker environment:

- RabbitMQ unavailable while 2,000 credits were accepted.
- RabbitMQ restarted with 500 queued messages.
- Worker terminated repeatedly while processing 8,000 credits.
- MongoDB unavailable for 30 seconds and 150 seconds.
- A backlog of 3,000 queued credits.
- Concurrent processing, duplicate delivery, and recovery after uncertain commits.

These drills completed with exact balances and without stranded credits.

The CI workflow in `.github/workflows/ci.yml` runs code quality checks, type checking, tests, image builds, and smoke tests. It also validates the Prometheus alert rules.

## Project Structure

```text
src/credit_service/
├── api/                  # FastAPI routes, validation, auth, health, metrics
├── services.py           # Credit workflows and business rules
├── repository.py         # MongoDB queries and transactions
├── worker.py             # Celery background tasks
├── celery_app.py         # RabbitMQ and Celery configuration
├── money.py              # Decimal validation
├── models.py             # Data models and statuses
├── db.py                 # MongoDB connections and indexes
├── config.py             # Environment configuration
├── errors.py             # Application errors
├── observability.py      # Structured logging
├── seed.py               # Demo users
└── unpark.py             # Operator recovery command

tests/
├── unit/                 # Validation and retry logic
└── integration/          # Real database, broker and worker tests

ops/                      # Prometheus alerts and alert tests
scripts/                  # Smoke test
docs/                     # Architecture and design document

compose.yml               # Local infrastructure
Dockerfile                # Application image
Makefile                  # Development commands
pyproject.toml            # Dependencies and tooling
uv.lock                   # Locked dependency versions
```

The application separates HTTP handling, business workflows, database operations, and background task execution. Business logic is shared between the API and worker rather than duplicated.

## Production Considerations

The application implements production-oriented reliability mechanisms, but the included Docker Compose environment is intended for local development and demonstration.

Before handling real financial traffic, the deployment would require:

- **High availability:** Multi-node MongoDB replica set, RabbitMQ quorum queues, and multiple API and worker instances.
- **Security:** Production authentication and authorization, TLS, secrets management, and private database and broker networking.
- **Monitoring:** Connect the existing metrics and alert rules to a production monitoring and notification system.
- **Data operations:** Backups, restore drills, retention policies, and balance reconciliation.
- **Capacity validation:** Sustained load testing, infrastructure failover testing, and production-specific performance tuning.

The existing group commit implementation reduces contention when many credits target the same account. However, the API still updates the user document for every accepted credit, which remains a throughput constraint for extremely busy accounts.

Local benchmarks are not production capacity guarantees.

For the full architecture, implementation rationale, testing strategy, and trade-offs, see the **[two-page design document](docs/design.pdf)**.

---

**Design principle:** Accept each credit durably, apply it at most once, and retain recoverable state until it reaches a valid outcome.