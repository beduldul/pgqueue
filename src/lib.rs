//! A job queue on top of Postgres. Correctness rests on two things:
//! `FOR UPDATE SKIP LOCKED` at claim time (two workers cannot take the same job),
//! and `locked_until` as a lease (a job from a dead worker comes back on its own).
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, FromRow, PgPool};
use std::time::Duration;

const SCHEMA: &str = include_str!("schema.sql");

#[derive(Debug, Clone, FromRow)]
pub struct Job {
    pub id: i64,
    pub queue: String,
    pub payload: Value,
    pub attempts: i32,
    pub max_attempts: i32,
    pub locked_by: Option<String>,
}

#[derive(Clone)]
pub struct Queue {
    pub pool: PgPool,
}

impl Queue {
    pub async fn connect(url: &str, max_conns: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_conns)
            .connect(url)
            .await
            .context("connect postgres")?;
        let q = Self { pool };
        q.migrate().await?;
        Ok(q)
    }

    pub async fn migrate(&self) -> Result<()> {
        // CREATE TABLE IF NOT EXISTS is not safe under concurrent migrators: Postgres
        // races on its internal catalogs and one of them fails with a duplicate key.
        // An advisory lock serialises them.
        const LOCK: i64 = 0x0070_6767_7175_6575;
        let mut conn = self.pool.acquire().await?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(LOCK)
            .execute(&mut *conn)
            .await?;
        let res = sqlx::raw_sql(SCHEMA).execute(&mut *conn).await;
        // The lock is released even when the migration fails, so a bad schema does not
        // wedge every later attempt.
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(LOCK)
            .execute(&mut *conn)
            .await
            .ok();
        res?;
        Ok(())
    }

    pub async fn enqueue(&self, queue: &str, payload: Value, max_attempts: i32) -> Result<i64> {
        let id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO jobs (queue, payload, max_attempts) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(queue)
        .bind(payload)
        .bind(max_attempts)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    /// Claim up to `limit` ready jobs. `lease` is how long the worker is trusted to hold
    /// them before `reclaim` gives them back.
    pub async fn claim(
        &self,
        queue: &str,
        worker: &str,
        lease: Duration,
        limit: i64,
    ) -> Result<Vec<Job>> {
        let until: DateTime<Utc> = Utc::now() + chrono::Duration::from_std(lease)?;
        let rows = sqlx::query_as::<_, Job>(
            r#"
            WITH picked AS (
                SELECT id FROM jobs
                WHERE queue = $3 AND state = 'ready' AND run_at <= now()
                ORDER BY run_at
                FOR UPDATE SKIP LOCKED
                LIMIT $4
            )
            UPDATE jobs j
               SET state = 'running',
                   locked_by = $1,
                   locked_until = $2,
                   attempts = j.attempts + 1,
                   updated_at = now()
              FROM picked p
             WHERE j.id = p.id
            RETURNING j.id, j.queue, j.payload, j.attempts, j.max_attempts, j.locked_by
            "#,
        )
        .bind(worker)
        .bind(until)
        .bind(queue)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Return jobs whose lease expired (worker died or was SIGKILLed).
    /// Global by design: call it periodically from one place, not per worker.
    pub async fn reclaim(&self) -> Result<u64> {
        let r = sqlx::query(
            r#"UPDATE jobs
                  SET state = 'ready', locked_by = NULL, locked_until = NULL, updated_at = now()
                WHERE state = 'running' AND locked_until < now()"#,
        )
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected())
    }

    /// false when this worker no longer holds the job (its lease expired and someone
    /// else took over). This guard is what stops a slow worker from clobbering a fresh one.
    pub async fn complete(&self, id: i64, worker: &str) -> Result<bool> {
        let r = sqlx::query(
            r#"UPDATE jobs
                  SET state = 'done', locked_by = NULL, locked_until = NULL, updated_at = now()
                WHERE id = $1 AND state = 'running' AND locked_by = $2"#,
        )
        .bind(id)
        .bind(worker)
        .execute(&self.pool)
        .await?;
        Ok(r.rows_affected() == 1)
    }

    /// Retry with a `2^attempts` second backoff (capped at 300), or mark dead once
    /// attempts run out. Returns the resulting state: "ready" or "dead".
    pub async fn fail(&self, id: i64, worker: &str, err: &str) -> Result<String> {
        let state = sqlx::query_scalar::<_, String>(
            r#"UPDATE jobs
                  SET state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'ready' END,
                      run_at = CASE WHEN attempts >= max_attempts THEN run_at
                                    ELSE now() + (least(power(2, attempts), 300) * interval '1 second') END,
                      locked_by = NULL,
                      locked_until = NULL,
                      last_error = $3,
                      updated_at = now()
                WHERE id = $1 AND state = 'running' AND locked_by = $2
            RETURNING state"#,
        )
        .bind(id)
        .bind(worker)
        .bind(err)
        .fetch_optional(&self.pool)
        .await?
        .context("job is not held by this worker")?;
        Ok(state)
    }

    /// (state, attempts) — used by tests and by operational checks.
    pub async fn state(&self, id: i64) -> Result<(String, i32)> {
        let row =
            sqlx::query_as::<_, (String, i32)>("SELECT state, attempts FROM jobs WHERE id = $1")
                .bind(id)
                .fetch_one(&self.pool)
                .await?;
        Ok(row)
    }

    pub async fn pending(&self, queue: &str) -> Result<i64> {
        let n = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM jobs WHERE queue = $1 AND state IN ('ready','running')",
        )
        .bind(queue)
        .fetch_one(&self.pool)
        .await?;
        Ok(n)
    }
}
