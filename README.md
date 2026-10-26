# Async Credit Processing Service

A client asks to credit a user's balance. The API records the request and answers `202 Accepted` straight away, and a Celery worker applies the credit in the background. Each credit is applied **at most once**. That holds even when RabbitMQ redelivers a message, a worker crashes mid-task, a commit reply is lost, or the client retries. A credit that was accepted is never dropped: if its message is lost or never published, it is published again.

**Stack:** Python 3.12, FastAPI, Celery, RabbitMQ, MongoDB 8.2 (replica set), Docker Compose

The design and its trade-offs are explained in [docs/design.pdf](docs/design.pdf).

```
client ──POST /credits──▶ API ──(1) txn──▶ MongoDB   credit: processing · user: pending_credits+1
                           └──(2) publish id, confirmed──▶ RabbitMQ ──▶ Celery worker
                                                                           │
                MongoDB ◀──(3) one txn: credit processed + balance += amount ┘ (ack after commit)
   beat ──every 15s──▶ dispatcher: publishes credits whose publish was never confirmed
```

## Quick start

You need **Docker** with Compose v2. To run the tests you also need [uv](https://docs.astral.sh/uv/) (`brew install uv`).

```bash
make up      # build and start MongoDB, RabbitMQ, API, worker, beat; waits until healthy
make seed    # create demo users u_123 (balance 100.00), u_new (no balance yet), u_alice
make smoke   # submit a credit end to end and check the balance moved by exactly 50.00
```

| What | Where |
|---|---|
| Swagger UI | http://localhost:8000/docs. Click **Authorize** and enter `local-dev-key`. |
| Prometheus metrics | http://localhost:8000/metrics |
| RabbitMQ management | http://localhost:15672 (guest / guest) |
| MongoDB | `mongodb://localhost:27017/?replicaSet=rs0&directConnection=true` |

`make logs` follows all the services. `make down` stops everything and **deletes the data volumes**.

## API

Every `/credits` and `/users` call needs an `X-API-Key` header. Compose configures one client, `reviewer:local-dev-key`; set your own with `API_KEYS=client:key,...`.

### `POST /credits`: submit a credit

```bash
curl -i -X POST localhost:8000/credits \
  -H 'X-API-Key: local-dev-key' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 6f1c2a1e-order-42' \
  -d '{"user_id": "u_new", "amount": "50.00"}'
```

```http
HTTP/1.1 202 Accepted
location: /credits/cr_09c88385ea4a4193885f78f8caf8bbf3

{"task_id": "cr_09c88385ea4a4193885f78f8caf8bbf3", "status": "processing", "status_url": "/credits/cr_09c88385ea4a4193885f78f8caf8bbf3"}
```

| Status | When |
|---|---|
| `202` | The credit was accepted. It is saved in MongoDB, even if the publish to RabbitMQ failed. |
| `401` | The API key is missing or wrong. |
| `404` | The user doesn't exist. Nothing is saved. |
| `409` | This client already used the `Idempotency-Key` with a different body. |
| `422` | The amount is invalid. It must be greater than 0, at most 1,000,000.00, with at most two decimal places. |
| `429` | The user already has 1,000 credits in flight. Nothing is saved; retry after `Retry-After`. |
| `503` | The queue is backlogged (nothing is saved), or MongoDB was briefly unavailable. Retry with the **same** `Idempotency-Key`. |

`Idempotency-Key` is optional and scoped to the API client. If you retry with the same key and body, you get the original `task_id` back.

### `GET /credits/{task_id}`: credit status

```json
{
  "task_id": "cr_09c88385ea4a4193885f78f8caf8bbf3",
  "user_id": "u_new",
  "amount": "50.00",
  "status": "processed",
  "attempts": 1,
  "error": null,
  "created_at": "2026-10-26T12:37:38.351000Z",
  "processed_at": "2026-10-26T12:37:38.359000Z",
  "published_at": "2026-10-26T12:37:38.357000Z"
}
```

`status` is `processing`, `processed` (the balance has been updated) or `failed` (the balance was not changed, and `error` says why). `published_at` is when RabbitMQ confirmed the message.

### `GET /users/{user_id}`: balance

```json
{"user_id": "u_new", "balance": "50.00", "status": "processed", "pending_credits": 0}
```

### Operations

- `GET /health/live`: returns 200 while the process is running. It doesn't check any dependencies.
- `GET /health/ready`: checks MongoDB and RabbitMQ. It returns 503 if either one can't be reached.
- `GET /metrics`: Prometheus metrics. These gauges are read from MongoDB, so they cover every process: `credit_processing`, `credit_unpublished`, `credit_failed`, `credit_oldest_processing_age_seconds`, `credit_oldest_unpublished_age_seconds` and `credit_queue_depth`. There is also an HTTP latency histogram. Alert on the oldest-age gauges and on `credit_failed` going up.

## How it works

1. **Accept.** Requests are checked first: if the queue is backlogged the API returns `503`, and if the user already has too many credits in flight it returns `429`. Nothing is saved in either case. Then one transaction inserts the credit (`processing`) and increments the user's `pending_credits`. The API publishes only the credit ID to RabbitMQ and waits for the broker to confirm it, then records `published_at`. If publishing fails, the API still returns 202, and it stops trying to publish for 5s so that later requests don't each wait on a broker that is down.
2. **Apply.** The worker runs one transaction that moves the credit from `processing` to `processed` **only if it is still `processing`**, adds the amount to the balance (creating the field if needed) and decrements `pending_credits`. The worker acks the message only after this commits, so a redelivered or duplicate message does nothing.
3. **Dispatch.** Every 15s, Celery beat runs the dispatcher. It claims due credits in batches of 500, oldest first. Each claim gets a unique token and a 60s lease, so two dispatchers can't take the same credit. It keeps going until nothing is due, for up to 10s per run. There are two kinds of due credits:
   - **Never confirmed by the broker:** published after a 10s grace period. This covers a broker outage and an API crash between commit and publish.
   - **Confirmed but still unprocessed after 15 minutes:** republished **only while the queue is (nearly) empty**. When the queue is deep, the message is almost certainly still waiting in it, and republishing would only make the backlog worse.
4. **Failure policy.** Infrastructure errors **never** fail a credit. Transient MongoDB errors are retried up to 5 times with exponential backoff and jitter; after that the dispatcher picks the credit up again. A business error, such as the user having been deleted, fails the credit immediately. An execution that crashes the worker is counted as a crash: RabbitMQ marks the redelivered message, and the earlier execution never finished. An unexpected exception also counts as a crash. After 3 crashes the credit is marked `failed`. A failed credit leaves the balance unchanged and releases its `pending_credits`.

## Tests

The integration tests run against real MongoDB and RabbitMQ, because transactions, acks and redelivery can't be tested properly with mocks. They use a separate database (`credit_service_test`) and queue (`credits_test`).

```bash
uv sync
make up            # or just: docker compose up -d --wait mongo rabbitmq
make check         # ruff + mypy --strict + all 85 tests (about 8s)
make test-unit     # no infrastructure needed
```

| Scenario | How it's tested | Expected result |
|---|---|---|
| Worker crashes after commit, before ack | A real consumer takes the message, commits, then drops its connection; the real worker gets the redelivered message | Credited once; the redelivery does nothing |
| Commit reply lost (`UnknownTransactionCommitResult`) in the worker | Commit succeeds, then the error is injected | Retried; the second run does nothing; balance, status and pending count are all correct |
| Commit reply lost at the API | Same injection | `503`; a retry with the same key gets the same credit; the dispatcher publishes it |
| Same message delivered twice, or to 8 workers at once | Direct calls and threads | Exactly one applies it |
| Apply and fail racing on the same credit | Threads | One terminal state; balance and pending count agree with it |
| RabbitMQ down when the API publishes | Failing publisher, and a real worker | Dispatched after the grace period, then applied |
| 10,000 never-published credits | Bulk insert, one dispatcher run | All published, none twice, in under 10s |
| 4 dispatchers running at once over 2,000 credits | Threads | No credit published twice |
| Dispatcher dies after publish, before recording it | Injected failure | Republished once more; credited once |
| Deep queue with old confirmed credits | Queue depth set to unknown, 5,000, or 0 | Republished only when the queue is drained |
| Long MongoDB outage (20 transient failures) | Repeated failures | Never marked failed; applied once the database is back |
| Poison message: repeated crashes or unexpected errors | Simulated redeliveries | `failed` after 3; balance unchanged |
| Client retries; same key with a different body; same key from another client | API | Same credit / `409` / a separate credit |
| Too many credits in flight for one user; queue backlogged | API | `429` / `503`; nothing saved |
| 40 parallel submit+apply; 30 credits through an 8-thread worker | Threads, real worker | Exact to the cent; **invariants checked from snapshots while processing runs** |
| Invalid amounts, unknown users, missing API key | API | `422` / `404` / `401` |

The key guards were checked by breaking them on purpose. Removing the `processing` guard, ignoring the redelivered flag, republishing regardless of queue depth, or counting transient errors as crashes each makes the matching tests fail.

Also checked by hand on the Docker stack:
- **RabbitMQ stopped while 2,000 credits came in:** all 2,000 got `202` in 5s, and all were applied within 9s of RabbitMQ coming back.
- **Workers stopped with 3,000 credits queued:** nothing was republished past the threshold, and there were 0 duplicate deliveries once the workers came back.

## Decisions and assumptions

- **Money uses `Decimal`** in Python and `Decimal128` in MongoDB, never floats. The API returns money as strings (`"150.00"`). The example in the brief shows a number, so this is a deliberate difference.
- **Credits for unknown users are rejected** with `404`. Accounts aren't created implicitly. `make seed` creates demo users.
- **Each credit has its own status in `credit_operations`, and that is the authoritative status.** The user's `status` is `processing` while any credit is in flight. Once none are, it shows the outcome of the last one to finish.
- **The credit document is also the outbox.** The API still publishes directly, as the brief asks. The dispatcher only handles credits whose publish wasn't confirmed, so there's no separate outbox table or relay service.
- **The API uses synchronous handlers on purpose.** PyMongo and Celery publishing are blocking calls, so FastAPI runs the handlers in its threadpool and the event loop isn't blocked.
- **Authentication is a minimal API key.** There's no rule about which client may credit which user; that would come from the real platform's identity model. Idempotency keys are unique per client and are kept as part of the audit record.
- **MongoDB 8.2, not 8.0.** Current 8.0.x builds refuse to start on Linux kernels 6.19 and later ([SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912)), and recent Docker Desktop VMs use those kernels.

## Known limits and production deployment

- **A very busy single account is a bottleneck.** Every credit writes the same user document, so throughput for one user is limited (about 100–150 credits/s on a laptop). `429` stops its backlog from growing without limit. If production needed more, credits could become ledger entries with the balance computed from them. That changes the `balance` contract, so it isn't done here.
- **Measured locally:** about 315 requests/s accepted across 100 users, with 1 API process and 4 workers. That's a laptop number, not a capacity guarantee. Production capacity would come from running more API and worker instances, and should be benchmarked on real hardware.
- **High availability:** production needs a 3-node replica set with majority writes, RabbitMQ quorum queues, several API and worker instances, and a single beat (concurrent dispatchers are safe, but you only need one).
- **Retention:** credit records are the audit trail and shouldn't be deleted by a TTL. They would be archived according to a retention policy, with a reconciliation job that checks balances against processed credits.

## Configuration

These environment variables are read by the API, the worker and beat. Local runs also read a `.env` file (see `.env.example`).

| Variable | Default | Purpose |
|---|---|---|
| `MONGO_URI` / `MONGO_DB` | localhost replica set / `credit_service` | Must point to a replica set (needed for transactions) |
| `BROKER_URL` / `CREDITS_QUEUE` | local RabbitMQ / `credits` | |
| `API_KEYS` | empty (auth off) | `client:key,...`; compose sets `reviewer:local-dev-key` |
| `MAX_PENDING_PER_USER` / `MAX_QUEUE_DEPTH` | `1000` / `100000` | Admission limits (`429` / `503`) |
| `PUBLISH_PAUSE_SECONDS` | `5` | How long the API stops publishing after a failed publish |
| `TASK_MAX_RETRIES` / `RETRY_BACKOFF_SECONDS` / `RETRY_BACKOFF_MAX_SECONDS` | `5` / `1` / `60` | Retries for transient errors |
| `MAX_CRASHES` / `TASK_TIME_LIMIT_SECONDS` | `3` / `300` | Crash budget for poison messages, and the hard time limit per task |
| `DISPATCH_INTERVAL_SECONDS` / `DISPATCH_TIME_BUDGET_SECONDS` / `DISPATCH_BATCH_SIZE` | `15` / `10` / `500` | How often the dispatcher runs, how long each run may take, and the batch size |
| `PUBLISH_GRACE_SECONDS` / `REPUBLISH_AFTER_SECONDS` / `RECONCILE_MAX_QUEUE_DEPTH` | `10` / `900` / `100` | When unconfirmed and confirmed credits become due, and the queue depth below which confirmed credits may be republished |
| `DISPATCH_LEASE_SECONDS` | `60` | How long a dispatcher's claim lasts |
| `LOG_LEVEL` / `LOG_FORMAT` | `INFO` / `json` | Use `console` for readable local logs |

## Project layout

```
src/credit_service/
  api/              FastAPI app, routes, schemas, auth, error mapping, health, metrics
  services.py       credit workflows: admission, failure policy, dispatcher (no FastAPI/Celery)
  repository.py     MongoDB queries and transactions
  worker.py         Celery tasks: apply_credit, dispatch_pending_credits
  celery_app.py     Celery/RabbitMQ configuration, publishing, queue-depth probe
  money.py          amount parsing and validation
  db.py, models.py, errors.py, config.py, observability.py, seed.py
tests/unit/         pure logic: money, backoff, error classification
tests/integration/  real MongoDB + RabbitMQ, including an in-process Celery worker
```
