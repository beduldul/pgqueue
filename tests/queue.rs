// These run against a real Postgres, not mocks.
// Set QUEUE_TEST_URL to point at another database.
use anyhow::Result;
use pgqueue::Queue;
use serde_json::json;
use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

async fn q(conns: u32) -> Result<Queue> {
    let url = std::env::var("QUEUE_TEST_URL")
        .unwrap_or_else(|_| "postgresql://hindsight:hindsight@127.0.0.1:5432/pgqueue_test".into());
    Queue::connect(&url, conns).await
}

async fn clear(qq: &Queue, queue: &str) -> Result<()> {
    sqlx::query("DELETE FROM jobs WHERE queue = $1")
        .bind(queue)
        .execute(&qq.pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn enqueue_claim_complete() -> Result<()> {
    let qq = q(2).await?;
    clear(&qq, "t_basic").await?;
    let id = qq.enqueue("t_basic", json!({"n": 1}), 3).await?;

    let jobs = qq
        .claim("t_basic", "w1", Duration::from_secs(30), 10)
        .await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, id);
    assert_eq!(jobs[0].attempts, 1);

    assert!(qq.complete(id, "w1").await?);
    assert_eq!(qq.state(id).await?.0, "done");
    // finished jobs are never claimed again
    assert!(qq
        .claim("t_basic", "w1", Duration::from_secs(30), 10)
        .await?
        .is_empty());
    assert_eq!(qq.pending("t_basic").await?, 0);
    Ok(())
}

#[tokio::test]
async fn no_double_delivery() -> Result<()> {
    const N: usize = 200;
    const WORKERS: usize = 8;
    let qq = q(WORKERS as u32 + 2).await?;
    clear(&qq, "t_race").await?;

    for i in 0..N {
        qq.enqueue("t_race", json!({"i": i}), 3).await?;
    }

    let seen = Arc::new(Mutex::new(Vec::<i64>::with_capacity(N)));
    let done = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for w in 0..WORKERS {
        let qq = qq.clone();
        let seen = seen.clone();
        let done = done.clone();
        let name = format!("w{w}");
        handles.push(tokio::spawn(async move {
            loop {
                let got = qq
                    .claim("t_race", &name, Duration::from_secs(30), 5)
                    .await
                    .unwrap();
                if got.is_empty() {
                    if done.load(Ordering::SeqCst) >= N {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }
                for j in got {
                    // lock held only briefly, no await inside
                    seen.lock().unwrap().push(j.id);
                    assert!(
                        qq.complete(j.id, &name).await.unwrap(),
                        "failed to complete {}",
                        j.id
                    );
                    done.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    for h in handles {
        h.await?;
    }

    let ids = seen.lock().unwrap().clone();
    let unique: HashSet<i64> = ids.iter().copied().collect();
    assert_eq!(ids.len(), N, "claim count must be exactly {N}");
    assert_eq!(unique.len(), N, "a job was claimed by two workers");
    assert_eq!(qq.pending("t_race").await?, 0);
    Ok(())
}

#[tokio::test]
async fn dead_worker_is_returned() -> Result<()> {
    let qq = q(2).await?;
    clear(&qq, "t_crash").await?;
    let id = qq.enqueue("t_crash", json!({"x": true}), 5).await?;

    // 1 second lease, then "die" without completing
    let got = qq
        .claim("t_crash", "w1", Duration::from_secs(1), 10)
        .await?;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].attempts, 1);

    tokio::time::sleep(Duration::from_millis(1200)).await;
    // reclaim is global, so it also returns leases belonging to tests running in
    // parallel. Filter down to the job this test actually owns.
    qq.reclaim().await?;

    let got = qq
        .claim("t_crash", "w2", Duration::from_secs(30), 10)
        .await?;
    let mine: Vec<_> = got.iter().filter(|j| j.id == id).collect();
    assert_eq!(
        mine.len(),
        1,
        "the job must come back after its lease expires"
    );
    assert_eq!(mine[0].attempts, 2, "attempt counter must advance");
    Ok(())
}

#[tokio::test]
async fn stale_worker_cannot_overwrite() -> Result<()> {
    let qq = q(2).await?;
    clear(&qq, "t_stale").await?;
    let id = qq.enqueue("t_stale", json!({}), 5).await?;

    qq.claim("t_stale", "w1", Duration::from_secs(1), 10)
        .await?;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    qq.reclaim().await?;

    // w2 takes over
    let got = qq
        .claim("t_stale", "w2", Duration::from_secs(30), 10)
        .await?;
    assert_eq!(got.len(), 1);

    // w1 wakes up and tries to complete -> must be rejected
    assert!(
        !qq.complete(id, "w1").await?,
        "w1 must not overwrite w2's result"
    );
    assert!(qq.complete(id, "w2").await?);
    assert_eq!(qq.state(id).await?.0, "done");
    Ok(())
}

#[tokio::test]
async fn retry_backoff_then_dead() -> Result<()> {
    let qq = q(2).await?;
    clear(&qq, "t_retry").await?;
    let id = qq.enqueue("t_retry", json!({}), 2).await?;

    // attempt 1 -> fail -> ready again
    qq.claim("t_retry", "w1", Duration::from_secs(30), 10)
        .await?;
    assert_eq!(qq.fail(id, "w1", "boom").await?, "ready");
    assert_eq!(qq.state(id).await?, ("ready".to_string(), 1));

    // backoff is 2^1 = 2 seconds, so it must not be claimable immediately
    assert!(
        qq.claim("t_retry", "w1", Duration::from_secs(30), 10)
            .await?
            .is_empty(),
        "job must not be ready before backoff elapses"
    );

    tokio::time::sleep(Duration::from_millis(2100)).await;

    // attempt 2 -> attempts == max -> dead
    let got = qq
        .claim("t_retry", "w1", Duration::from_secs(30), 10)
        .await?;
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].attempts, 2);
    assert_eq!(qq.fail(id, "w1", "boom again").await?, "dead");
    assert_eq!(qq.state(id).await?.0, "dead");
    Ok(())
}
