# Async Credit Processing Service — Design

## 1. Problem and scope

Build an asynchronous API that accepts a credit, returns **202 Accepted** with a task ID, and updates an existing user's MongoDB balance in the background. The key requirements are **no duplicate balance effects**, **durable accepted requests**, and **exact decimal money**. Messages can be redelivered; correctness is enforced in MongoDB rather than assumed from the broker.

**Assumptions:** Existing users only (404 otherwise); one currency; positive amounts up to 1,000,000.00 with at most two decimal places. A demo API key identifies clients; account-level authorization is outside this exercise.

## 2. Components and request flow

```text
Client → FastAPI → MongoDB (credit + pending count in one transaction)
                ↘ RabbitMQ (credit ID only) → Celery worker
                                             ↘ MongoDB (balance + status in one transaction)
Celery Beat (15s) → dispatcher → RabbitMQ (recovery)
```

1. **Accept.** FastAPI validates the API key, amount and admission limits. It records the operation as `processing` and increments the user's `pending_credits` in one MongoDB transaction. It publishes the operation ID to RabbitMQ and returns `202`. If the broker is down, the already-durable operation remains recoverable.
2. **Apply.** Celery loads the credit from MongoDB, conditionally changes `processing → processed`, updates the balance, and adjusts the pending counter in one transaction. Acknowledgment happens only after commit. **Group commit** can settle up to 100 credits for one user in the same transaction, reducing hot-account contention.
3. **Recover.** The dispatcher runs every 15 seconds. It claims unconfirmed operations using tokens and expiring leases, then publishes in bounded batches. Old confirmed but unsettled operations are reconsidered after 15 minutes only when the queue is nearly drained; this avoids replay storms during backlogs.

**Core components:** FastAPI (HTTP/validation); MongoDB 8.2 replica set (source of truth); RabbitMQ (durable queue, persistent messages and publisher confirms); Celery worker (late acknowledgments, retries); Celery Beat (dispatcher schedule). API, worker and Beat share one codebase; business workflows are independent of FastAPI/Celery.

## 3. Data and correctness

- `users`: unique `user_id`, `balance` (Decimal128), aggregate `status`, `pending_credits`.
- `credit_operations`: operation/task ID, client and user IDs, amount, authoritative status, per-client idempotency key and request fingerprint, timestamps, attempt/crash information and dispatch lease/confirmation fields. The operation itself serves as the outbox record.
- Python `Decimal` is stored as BSON `Decimal128` and returned as JSON strings. A per-user status cannot represent multiple simultaneous credits; each operation has its own status.

**Exactly-once business effect:** RabbitMQ delivery is at-least-once. The guarded operation transition and balance adjustment commit atomically, so duplicates and redeliveries never add the same credit twice. Idempotency keys prevent clients from creating duplicate operations when retrying uncertain HTTP requests.

## 4. Failure and overload policy

| Scenario | Behavior |
|---|---|
| Worker dies after commit but before ACK | RabbitMQ redelivers; already-processed operation is a no-op. |
| Broker down or API crashes after saving | Dispatcher republishes the durable operation. |
| MongoDB timeout, transient error or unknown commit result | Retry or resolve from durable state; do not fail an accepted credit due to an infrastructure outage. |
| Repeated worker crashes | After 10 crashes, park the still-owed credit, alert an operator and require explicit unpark. |
| User removed before settlement | Mark a non-recoverable business failure without crediting balance. |
| High load | Reject before saving: 429 at 1,000 pending credits/user, 503 for excessive queue backlog. |

MongoDB uses explicit majority-journaled writes and transaction snapshot reads. The API has a 10-second aggregate MongoDB deadline; worker transaction retries can last longer. Readiness, JSON logs, metrics and tested Prometheus alert rules support recovery.

## 5. Verification

**94 automated tests** cover money precision, idempotency races, concurrent balance updates, guarded terminal transitions, group commit, recovery leases, retries, endpoint limits and complete API-to-worker processing. Integration/E2E tests use real MongoDB and RabbitMQ. Fault injection covers commit-reply loss and crashes between publication, commit and acknowledgment. Invariants are checked during concurrent execution: **balance = opening balance + processed credits**, and **pending count = unsettled operations**.

Real-stack drills included RabbitMQ down during 2,000 accepted credits, 500 queued messages surviving restart, repeated SIGKILL during 8,000 credits, MongoDB outages of 30 and 150 seconds, and a 3,000-message backlog without a republish storm. Local CI-equivalent checks cover lint, strict typing, alert-rule tests, application tests, image build and smoke test.

## 6. Trade-offs and production deployment

- The local Docker Compose deployment uses one MongoDB replica-set member and one RabbitMQ node; it is **not highly available**. Production needs replica-set redundancy, RabbitMQ quorum queues, multiple API/worker instances, secrets, TLS, client authorization, backups and recovery drills.
- Group commit improved measured hot-account settlement from 76 to 145 credits/s on a laptop, but accepting each credit still writes the user record. Roughly 300 requests/s overall is a local benchmark, **not a production capacity guarantee**.
- Parked operations must be monitored and manually released. A live production system also needs deployed dashboards/alert routing, retention/archiving rules, reconciliation jobs and sustained failover/soak testing.
