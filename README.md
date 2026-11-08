# pgqueue

A job queue for Rust on top of Postgres. No Redis, no RabbitMQ, no Kafka — if you already run Postgres, you already run the queue.

## Guarantees

| Property | How |
|---|---|
| A job is never handed to two workers | `FOR UPDATE SKIP LOCKED` inside a single `UPDATE ... RETURNING` — never `SELECT` then `UPDATE` |
| A dead worker does not lose the job | `locked_until` acts as a lease; `reclaim()` returns jobs whose lease expired |
| A slow worker cannot overwrite a newer one | `complete()` / `fail()` only apply when `locked_by` still matches |
| Retries do not pile up | backoff of `2^attempts` seconds, capped at 300 |
| A poisoned job stops | after `max_attempts` the state becomes `dead` — kept, not deleted, so it can be inspected |
| Concurrent migrations are safe | `migrate()` serialises on a Postgres advisory lock |

## Status & honesty

The guarantees above are tested against a real Postgres instance, not mocks. `tests/queue.rs` covers the claim that matters most: **200 jobs distributed across 8 concurrent workers produce exactly 200 unique claims and zero duplicates.**

What this is *not*:

- **Not benchmarked.** No throughput numbers (jobs/sec) are published, because none have been measured.
- **Not tested against a Postgres restart mid-run.** Lease expiry is tested; a primary failover is not.
- **`reclaim()` is a global scan.** It sweeps every `running` row on each call. At large volumes, schedule it (once a second, or on the leader only) rather than calling it in a tight loop from every worker.
- **At-least-once, not exactly-once.** A worker that commits its side effect and dies before `complete()` will have that job run again. Handlers must be idempotent. This is a property of every queue of this shape, not a bug to be fixed later.

## Usage

```rust
use pgqueue::Queue;
use serde_json::json;
use std::time::Duration;

let q = Queue::connect("postgresql://user:pass@127.0.0.1:5432/app", 10).await?;

// producer
q.enqueue("email", json!({"to": "a@b.c"}), 5).await?;   // max_attempts = 5

// worker
loop {
    for job in q.claim("email", "worker-1", Duration::from_secs(60), 10).await? {
        match send(&job.payload).await {
            Ok(_)  => q.complete(job.id, "worker-1").await?,
            Err(e) => q.fail(job.id, "worker-1", &e.to_string()).await?,
        };
    }
    q.reclaim().await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

Call `reclaim()` periodically from one process; it is safe to call repeatedly.

The `lease` argument is the promise "I will finish or fail within this long". Set it above your p99 handler time. Too short and jobs run twice; too long and a crashed worker stalls its job for that duration.

## State machine

```
ready --claim--> running --complete--> done
                    |
                    +--fail (attempts remain)--> ready (run_at pushed back)
                    +--fail (attempts exhausted)--> dead
```

## Schema

One table, `jobs`. Two partial indexes: one for the claim path (`state = 'ready'`), one for lease recovery (`state = 'running'`). Both are partial so they stay small regardless of how many finished rows accumulate.

## Tests

```sh
export QUEUE_TEST_URL="postgresql://user:pass@127.0.0.1:5432/pgqueue_test"
cargo test
```

| Test | What it proves |
|---|---|
| `enqueue_claim_complete` | the normal cycle; a finished job cannot be claimed again |
| `no_double_delivery` | 200 jobs / 8 concurrent workers → 200 claims, 200 unique |
| `dead_worker_is_returned` | an expired lease is reclaimed and the attempt counter advanced |
| `stale_worker_cannot_overwrite` | a worker that wakes up after losing its lease is rejected |
| `retry_backoff_then_dead` | backoff actually delays the next claim; exhaustion lands in `dead` |

## Deliberately not built

Nothing here exists until something needs it: periodic/cron jobs, priorities, unique-job deduplication, Prometheus metrics, and job cancellation.

## License

MIT
