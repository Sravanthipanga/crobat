-- Crobat Cassandra schema: append-only run history.
-- Partition key (tenant_id, task_id, month) matches the Job Service's
-- only access pattern: "history for this task". Rows carry a 30-day TTL
-- applied at insert time (see adapters/cassandra), so history is a
-- rolling operational window.

CREATE KEYSPACE IF NOT EXISTS crobat
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE crobat;

CREATE TABLE IF NOT EXISTS job_history (
    tenant_id    text,
    task_id      text,
    month        text,       -- 'YYYY-MM' of finished_at, part of the partition key
    finished_at  timestamp,
    job_id       text,
    scheduled_at timestamp,
    status       text,       -- succeeded | dead
    attempts     int,
    http_status  int,
    last_error   text,
    PRIMARY KEY ((tenant_id, task_id, month), finished_at, job_id)
) WITH CLUSTERING ORDER BY (finished_at DESC, job_id ASC);
