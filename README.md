# Async Credit Processing Service

A client asks to credit a user's balance; the API records the request and answers `202 Accepted` immediately, and a Celery worker applies the credit in the background. Every credit is applied **at most once**, even when RabbitMQ redelivers a message, a worker crashes mid-task, or the client retries.

**Stack:** Python 3.12 · FastAPI · Celery · RabbitMQ · MongoDB 8.2 (replica set) · Docker Compose

The design and its trade-offs are explained in [docs/design.pdf](docs/design.pdf).

```
client ──POST /credits──▶ API ──(1) transaction──▶ MongoDB   credit_operations: processing
                           │                                  users: status=processing
                           └──(2) publish task id──▶ RabbitMQ ──▶ Celery worker
                                                                     │
                     MongoDB ◀──(3) one transaction: credit processed + balance += amount
```

## Quick start

Requirements: **Docker** with Compose v2. To run the tests you also need [uv](https://docs.astral.sh/uv/) (`brew install uv`).

```bash
make up      # build and start MongoDB, RabbitMQ, API, worker, beat; waits until healthy
make seed    # create demo users u_123 (balance 100.00), u_new (no balance yet), u_alice
make smoke   # submit a credit end to end and check the balance moved by exactly 50.00
```

| What | Where |
|---|---|
| Swagger UI (try the API here) | http://localhost:8000/docs |
| RabbitMQ management | http://localhost:15672 (guest / guest) |
| MongoDB | `mongodb://localhost:27017/?replicaSet=rs0&directConnection=true` |

`make logs` follows all services; `make down` stops everything and **deletes the data volumes**.

## API

### `POST /credits` — submit a credit

```bash
curl -i -X POST localhost:8000/credits \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 6f1c2a1e-order-42' \
  -d '{"user_id": "u_new", "amount": "50.00"}'
```

```http
HTTP/1.1 202 Accepted
location: /credits/cr_b521417468dc448c9e42a064d66868fb

{"task_id": "cr_b521417468dc448c9e42a064d66868fb", "status": "processing", "status_url": "/credits/cr_b521417468dc448c9e42a064d66868fb"}
```

- `amount`: a decimal string (`"50.00"`) or a number. It must be greater than 0 and at most 1,000,000.00, with no more than two decimal places. Invalid amounts get a `422`.
- `Idempotency-Key` (optional header): if you send the same key and body again, you get the original `task_id` back and no second credit is created. If you reuse the key with a different body, you get `409`.
- If `user_id` doesn't exist you get `404`, and nothing is recorded.

### `GET /credits/{task_id}`: credit status

```json
{
  "task_id": "cr_b521417468dc448c9e42a064d66868fb",
  "user_id": "u_new",
  "amount": "50.00",
  "status": "processed",
  "attempts": 1,
  "error": null,
  "created_at": "2026-10-26T11:33:28.916000Z",
  "processed_at": "2026-10-26T11:33:28.932000Z"
}
```

`status` is one of `processing`, `processed` (the balance has been updated) or `failed` (the balance was not changed, and `error` says why).

### `GET /users/{user_id}`: balance

```json
{"user_id": "u_new", "balance": "50.00", "status": "processed", "pending_credits": 0}
```

### Health

- `GET /health/live`: returns 200 while the process is running. It doesn't check any dependencies.
- `GET /health/ready`: checks MongoDB and RabbitMQ. It returns 503 if either one can't be reached.

## Tests

The integration tests run against real MongoDB and RabbitMQ, because transactions, acknowledgements and redelivery can't be checked properly with mocks. They use a separate database (`credit_service_test`) and queue (`credits_test`), so they don't touch the dev data or compete with the worker container for messages.

```bash
uv sync
make up            # or just: docker compose up -d --wait mongo rabbitmq
make check         # ruff + mypy --strict + all tests
make test-unit     # no infrastructure needed
```

What the suite covers:

| Scenario | Expected result |
|---|---|
| First credit for a user with no `balance` field | The field is created |
| Same message delivered twice / worker crashes after commit, before ack | Balance credited once |
| 8 workers handle the same message at the same time | Exactly one applies it |
| Client retry with the same `Idempotency-Key` (including 8 at once) | One credit, one publish |
| Same key, different body | `409` |
| RabbitMQ down when the API publishes | `202`; the sweeper republishes later; credit applied |
| Transient MongoDB error in the worker | Retried with backoff, then applied |
| User deleted before the credit is applied / too many attempts | `failed`, balance unchanged |
| 40 parallel submit+apply, and 30 credits through an 8-thread worker | Balance is exact to the cent |
| Amounts: negative, zero, more than 2 decimal places, NaN, booleans | `422` |

The full suite runs in about 5 seconds. CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs lint, type checks and the tests, then builds the images and runs the smoke test against the full stack.

## How it works

1. **Accept.** The API runs one MongoDB transaction that inserts a `credit_operations` document (`processing`), sets the user's `status` to `processing` and increments `pending_credits`. It then publishes only the operation id to RabbitMQ and waits for the broker to confirm. The operation id is also the Celery task id.
2. **Apply.** The worker runs one transaction that moves the operation from `processing` to `processed` **only if it is still `processing`**, adds the amount to the balance (creating the field if it's missing) and decrements `pending_credits`. A redelivered message finds the operation already settled and does nothing. That's why late acks (`acks_late`) are safe here.
3. **Recover.** Celery beat runs a sweeper every 30s. It republishes credits that have stayed in `processing` for more than 2 minutes, which covers both a failed publish and a lost message. Transient MongoDB errors are retried with exponential backoff and jitter. After 10 attempts in total, a credit is marked `failed`.

## Decisions and assumptions

- **Money uses `Decimal`** in Python and `Decimal128` in MongoDB, never floats. The API returns money as strings (`"150.00"`) so clients don't parse it as a binary float. The example in the brief shows a number, so this is a deliberate difference.
- **Credits for unknown users are rejected** with `404`. Accounts aren't created implicitly. `make seed` creates demo users.
- **Each credit has its own status in `credit_operations`, and that is the authoritative status.** The user's `status` follows the brief's flow. It is `processing` while any credit for that user is still in flight, and once none are, it shows the outcome of the last one to finish (`processed` or `failed`). One status field on the user can't describe several concurrent credits, so the per-credit record is the source of truth.
- **The API uses synchronous handlers on purpose.** PyMongo and Celery publishing are blocking calls, so FastAPI runs the handlers in its threadpool and the event loop isn't blocked.
- **Idempotency keys apply across all users and never expire.** The API has no authentication, so there is no client to scope keys to.
- **MongoDB 8.2, not 8.0.** Current 8.0.x builds refuse to start on Linux kernels 6.19 and later ([SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912)), and recent Docker Desktop VMs use those kernels.

## Configuration

These environment variables are read by the API, the worker and beat. Local runs also read a `.env` file (see `.env.example`).

| Variable | Default | Purpose |
|---|---|---|
| `MONGO_URI` | `mongodb://localhost:27017/?replicaSet=rs0&directConnection=true` | Must point to a replica set (needed for transactions) |
| `MONGO_DB` | `credit_service` | Database name |
| `BROKER_URL` | `amqp://guest:guest@localhost:5672//` | RabbitMQ |
| `CREDITS_QUEUE` | `credits` | Queue for credit tasks |
| `TASK_MAX_RETRIES` | `5` | Celery retries for transient MongoDB errors |
| `RETRY_BACKOFF_SECONDS` / `RETRY_BACKOFF_MAX_SECONDS` | `1` / `60` | Backoff base and cap |
| `MAX_ATTEMPTS` | `10` | Attempts across retries, redeliveries and requeues before a credit is marked `failed` |
| `SWEEP_INTERVAL_SECONDS` | `30` | How often the sweeper runs |
| `STALE_AFTER_SECONDS` | `120` | How long a credit can stay in `processing` before the sweeper republishes it |
| `LOG_LEVEL` / `LOG_FORMAT` | `INFO` / `json` | Use `console` for readable local logs |

## Project layout

```
src/credit_service/
  api/              FastAPI app, routes, request/response schemas, error mapping, health
  services.py       credit workflows (no FastAPI or Celery imports)
  repository.py     MongoDB queries and the transactions
  worker.py         Celery tasks: apply_credit, requeue_stale_credits
  celery_app.py     Celery/RabbitMQ configuration and publishing
  money.py          amount parsing and validation
  db.py, models.py, errors.py, config.py, observability.py, seed.py
tests/unit/         pure logic: money, backoff, error classification
tests/integration/  real MongoDB + RabbitMQ, including an in-process Celery worker
```
