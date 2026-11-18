-- Crobat MySQL schema (MySQL 8+).
-- The jobs table IS the queue: no broker, no outbox, nothing to fall
-- out of sync with the tasks table.

CREATE DATABASE IF NOT EXISTS crobat;
USE crobat;

-- Configuration: one row per scheduled task.
CREATE TABLE IF NOT EXISTS tasks (
    id               VARCHAR(32)  NOT NULL,
    tenant_id        VARCHAR(64)  NOT NULL,
    name             VARCHAR(255) NOT NULL,
    schedule_kind    VARCHAR(16)  NOT NULL,             -- now | at | cron
    run_at           DATETIME(6)  NULL,                 -- for kind = 'at'
    cron_expr        VARCHAR(128) NOT NULL DEFAULT '',
    timezone         VARCHAR(64)  NOT NULL DEFAULT '',
    priority         VARCHAR(16)  NOT NULL,             -- critical | high | normal | low
    req_method       VARCHAR(16)  NOT NULL,
    req_url          VARCHAR(2048) NOT NULL,
    req_headers      JSON         NOT NULL,             -- secret values stored encrypted (enc:v1:...)
    req_body         MEDIUMTEXT   NOT NULL,
    secret_headers   JSON         NOT NULL,             -- names of headers whose values are secrets
    timeout_ms       BIGINT       NOT NULL DEFAULT 0,
    max_attempts     INT          NOT NULL DEFAULT 1,
    backoff_base_ms  BIGINT       NOT NULL DEFAULT 0,
    backoff_max_ms   BIGINT       NOT NULL DEFAULT 0,
    next_run_at      DATETIME(6)  NULL,                 -- NULL once a one-shot task fired
    status           VARCHAR(16)  NOT NULL,             -- active | paused | deleted
    idempotency_key  VARCHAR(128) NOT NULL DEFAULT '',
    created_at       DATETIME(6)  NOT NULL,
    updated_at       DATETIME(6)  NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_tasks_tenant_idem (tenant_id, idempotency_key),
    KEY idx_tasks_due (status, next_run_at)
) ENGINE = InnoDB;

-- The hot queue table: one row per scheduled run.
-- id doubles as the Idempotency-Key sent to tenant endpoints.
CREATE TABLE IF NOT EXISTS jobs (
    id               VARCHAR(32)  NOT NULL,
    task_id          VARCHAR(32)  NOT NULL,
    tenant_id        VARCHAR(64)  NOT NULL,
    priority         VARCHAR(16)  NOT NULL,
    priority_rank    TINYINT      NOT NULL,             -- 0=critical .. 3=low, for ORDER BY
    req_method       VARCHAR(16)  NOT NULL,
    req_url          VARCHAR(2048) NOT NULL,
    req_headers      JSON         NOT NULL,
    req_body         MEDIUMTEXT   NOT NULL,
    secret_headers   JSON         NOT NULL,
    max_attempts     INT          NOT NULL DEFAULT 1,
    backoff_base_ms  BIGINT       NOT NULL DEFAULT 0,
    backoff_max_ms   BIGINT       NOT NULL DEFAULT 0,
    timeout_ms       BIGINT       NOT NULL DEFAULT 0,
    scheduled_at     DATETIME(6)  NOT NULL,
    status           VARCHAR(16)  NOT NULL,             -- pending | running | succeeded | dead
    attempt          INT          NOT NULL DEFAULT 0,   -- fencing token: incremented on every claim
    worker_id        VARCHAR(128) NULL,
    lease_expires_at DATETIME(6)  NULL,
    http_status      INT          NULL,
    last_error       TEXT         NOT NULL,
    created_at       DATETIME(6)  NOT NULL,
    finished_at      DATETIME(6)  NULL,
    PRIMARY KEY (id),
    KEY idx_jobs_claim (status, scheduled_at),
    KEY idx_jobs_tenant (tenant_id, status, scheduled_at),
    KEY idx_jobs_finished (status, finished_at)
) ENGINE = InnoDB;
