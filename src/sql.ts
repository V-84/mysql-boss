// §1.1 — DDL

export const CREATE_JOBS = `
CREATE TABLE IF NOT EXISTS jobs (
  id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  queue            VARCHAR(128)    NOT NULL,
  state            ENUM('available','active') NOT NULL DEFAULT 'available',
  priority         SMALLINT        NOT NULL DEFAULT 0,
  payload          JSON            NULL,
  singleton_key    VARCHAR(191)    NULL,
  retry_count      INT UNSIGNED    NOT NULL DEFAULT 0,
  retry_limit      INT UNSIGNED    NOT NULL DEFAULT 2,
  retry_delay_secs INT UNSIGNED    NOT NULL DEFAULT 30,
  retry_backoff    TINYINT(1)      NOT NULL DEFAULT 0,
  run_at           DATETIME(6)     NOT NULL,
  created_at       DATETIME(6)     NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
  started_at       DATETIME(6)     NULL,
  locked_by        BINARY(16)      NULL,
  lease_expires_at DATETIME(6)     NULL,
  last_error       JSON            NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_jobs_singleton (queue, singleton_key),
  KEY ix_jobs_dequeue (queue, state, priority DESC, run_at, id),
  KEY ix_jobs_lease   (state, lease_expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
`;

export const CREATE_JOBS_ARCHIVE = `
CREATE TABLE IF NOT EXISTS jobs_archive (
  id            BIGINT UNSIGNED NOT NULL,
  queue         VARCHAR(128)    NOT NULL,
  priority      SMALLINT        NOT NULL,
  payload       JSON            NULL,
  singleton_key VARCHAR(191)    NULL,
  retry_count   INT UNSIGNED    NOT NULL,
  created_at    DATETIME(6)     NOT NULL,
  started_at    DATETIME(6)     NOT NULL,
  completed_at  DATETIME(6)     NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
  duration_ms   INT UNSIGNED    NOT NULL,
  PRIMARY KEY (id),
  KEY ix_archive_queue_time (queue, completed_at),
  KEY ix_archive_time       (completed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
`;

export const CREATE_JOBS_DEAD = `
CREATE TABLE IF NOT EXISTS jobs_dead (
  id               BIGINT UNSIGNED NOT NULL,
  queue            VARCHAR(128)    NOT NULL,
  priority         SMALLINT        NOT NULL,
  payload          JSON            NULL,
  singleton_key    VARCHAR(191)    NULL,
  retry_count      INT UNSIGNED    NOT NULL,
  retry_limit      INT UNSIGNED    NOT NULL,
  retry_delay_secs INT UNSIGNED    NOT NULL,
  retry_backoff    TINYINT(1)      NOT NULL,
  created_at       DATETIME(6)     NOT NULL,
  failed_at        DATETIME(6)     NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
  last_error       JSON            NULL,
  PRIMARY KEY (id),
  KEY ix_dead_queue_time (queue, failed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
`;

export const CREATE_SCHEDULES = `
CREATE TABLE IF NOT EXISTS schedules (
  name             VARCHAR(128) NOT NULL,
  queue            VARCHAR(128) NOT NULL,
  cron             VARCHAR(64)  NOT NULL,
  timezone         VARCHAR(64)  NOT NULL DEFAULT 'UTC',
  payload          JSON         NULL,
  job_options      JSON         NULL,
  next_run_at      DATETIME(6)  NOT NULL,
  last_enqueued_at DATETIME(6)  NULL,
  updated_at       DATETIME(6)  NOT NULL DEFAULT (UTC_TIMESTAMP(6))
                                ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (name),
  KEY ix_schedules_due (next_run_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
`;

// §1.2 — Enqueue

export const ENQUEUE = `
INSERT INTO jobs
  (queue, priority, payload, singleton_key,
   retry_limit, retry_delay_secs, retry_backoff, run_at)
VALUES
  (?, ?, ?, ?, ?, ?, ?, COALESCE(?, UTC_TIMESTAMP(6)));
`;

export const ENQUEUE_SINGLETON = `
INSERT IGNORE INTO jobs
  (queue, priority, payload, singleton_key,
   retry_limit, retry_delay_secs, retry_backoff, run_at)
VALUES
  (?, ?, ?, ?, ?, ?, ?, COALESCE(?, UTC_TIMESTAMP(6)));
`;

export const LAST_INSERT_ID = `
SELECT CAST(LAST_INSERT_ID() AS CHAR) AS id;
`;

// §1.2 — Dequeue / claim

export const CLAIM_SELECT = `
SELECT CAST(id AS CHAR) AS id, queue, payload, retry_count, retry_limit,
       retry_delay_secs, retry_backoff
FROM jobs
WHERE queue = ?
  AND state = 'available'
  AND run_at <= UTC_TIMESTAMP(6)
ORDER BY priority DESC, run_at, id
LIMIT ?
FOR UPDATE SKIP LOCKED;
`;

export const CLAIM_UPDATE = `
UPDATE jobs
SET state = 'active',
    started_at = UTC_TIMESTAMP(6),
    locked_by = UUID_TO_BIN(?),
    lease_expires_at = UTC_TIMESTAMP(6) + INTERVAL ? SECOND
WHERE id IN (?);
`;

// §1.2 — Complete (archive move)

export const COMPLETE_ARCHIVE = `
INSERT INTO jobs_archive
  (id, queue, priority, payload, singleton_key, retry_count,
   created_at, started_at, duration_ms)
SELECT id, queue, priority, payload, singleton_key, retry_count,
       created_at, started_at,
       TIMESTAMPDIFF(MICROSECOND, started_at, UTC_TIMESTAMP(6)) DIV 1000
FROM jobs
WHERE id = ? AND locked_by = UUID_TO_BIN(?) AND state = 'active';
`;

export const COMPLETE_DELETE = `
DELETE FROM jobs
WHERE id = ? AND locked_by = UUID_TO_BIN(?) AND state = 'active';
`;

// §2.2 — Fail with retries remaining

export const FAIL_RETRY = `
UPDATE jobs
SET state = 'available',
    run_at = UTC_TIMESTAMP(6) + INTERVAL
      LEAST(
        86400,
        IF(retry_backoff = 1,
           retry_delay_secs * POW(2, retry_count),
           retry_delay_secs)
        + FLOOR(RAND() * retry_delay_secs)
      ) SECOND,
    retry_count = retry_count + 1,
    locked_by = NULL,
    lease_expires_at = NULL,
    started_at = NULL,
    last_error = ?
WHERE id = ?
  AND locked_by = UUID_TO_BIN(?)
  AND state = 'active'
  AND retry_count < retry_limit;
`;

// §3.2 — Dead-letter move

export const DLQ_INSERT = `
INSERT INTO jobs_dead
  (id, queue, priority, payload, singleton_key,
   retry_count, retry_limit, retry_delay_secs, retry_backoff,
   created_at, last_error)
SELECT id, queue, priority, payload, singleton_key,
       retry_count + 1, retry_limit, retry_delay_secs, retry_backoff,
       created_at, ?
FROM jobs
WHERE id = ? AND locked_by = UUID_TO_BIN(?) AND state = 'active';
`;

export const DLQ_DELETE = `
DELETE FROM jobs
WHERE id = ? AND locked_by = UUID_TO_BIN(?) AND state = 'active';
`;

// §3.2 — DLQ query

export const LIST_DEAD = `
SELECT CAST(id AS CHAR) AS id, queue, priority, payload, retry_count,
       UNIX_TIMESTAMP(created_at) AS created_at_unix,
       UNIX_TIMESTAMP(failed_at) AS failed_at_unix,
       last_error
FROM jobs_dead
WHERE queue = ? AND failed_at BETWEEN ? AND ?
ORDER BY failed_at DESC
LIMIT ? OFFSET ?;
`;

// §3.2 — Replay

export const REPLAY_INSERT = `
INSERT INTO jobs
  (queue, priority, payload, singleton_key,
   retry_limit, retry_delay_secs, retry_backoff, run_at)
SELECT queue, priority, payload, singleton_key,
       retry_limit, retry_delay_secs, retry_backoff, UTC_TIMESTAMP(6)
FROM jobs_dead
WHERE id IN (?)
FOR UPDATE;
`;

export const REPLAY_DELETE = `
DELETE FROM jobs_dead WHERE id IN (?);
`;

// §4.2 — Cron tick

export const DB_NOW = "SELECT UNIX_TIMESTAMP(UTC_TIMESTAMP(6)) AS db_now_unix;";

export const TICK_SELECT = `
SELECT name, queue, cron, timezone, payload, job_options,
       next_run_at,
       UNIX_TIMESTAMP(next_run_at) AS next_run_at_unix
FROM schedules
WHERE next_run_at <= UTC_TIMESTAMP(6)
ORDER BY next_run_at
LIMIT 20
FOR UPDATE SKIP LOCKED;
`;

export const TICK_ENQUEUE = `
INSERT IGNORE INTO jobs
  (queue, priority, payload, singleton_key,
   retry_limit, retry_delay_secs, retry_backoff, run_at)
VALUES
  (?, ?, ?, CONCAT('cron:', ?, ':', DATE_FORMAT(?, '%Y%m%d%H%i%s')),
   ?, ?, ?, UTC_TIMESTAMP(6));
`;

export const TICK_ADVANCE = `
UPDATE schedules
SET next_run_at = ?, last_enqueued_at = UTC_TIMESTAMP(6)
WHERE name = ?;
`;

// §4 — Schedule CRUD

export const SCHEDULE_UPSERT = `
INSERT INTO schedules (name, queue, cron, timezone, payload, job_options, next_run_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  queue = VALUES(queue),
  cron = VALUES(cron),
  timezone = VALUES(timezone),
  payload = VALUES(payload),
  job_options = VALUES(job_options),
  next_run_at = VALUES(next_run_at);
`;

export const SCHEDULE_DELETE = `
DELETE FROM schedules WHERE name = ?;
`;

// §6.1 — Heartbeat

export const HEARTBEAT = `
UPDATE jobs
SET lease_expires_at = UTC_TIMESTAMP(6) + INTERVAL ? SECOND
WHERE id IN (?) AND locked_by = UUID_TO_BIN(?) AND state = 'active';
`;

export const HEARTBEAT_OWNED = `
SELECT CAST(id AS CHAR) AS id
FROM jobs
WHERE id IN (?) AND locked_by = UUID_TO_BIN(?) AND state = 'active';
`;

// §6.2 — Drain release

export const DRAIN_RELEASE = `
UPDATE jobs
SET state = 'available', run_at = UTC_TIMESTAMP(6),
    locked_by = NULL, lease_expires_at = NULL, started_at = NULL
WHERE id IN (?) AND locked_by = UUID_TO_BIN(?) AND state = 'active';
`;

// §6.3 — Stale sweep

export const SWEEP_SELECT = `
SELECT CAST(id AS CHAR) AS id, retry_count, retry_limit
FROM jobs
WHERE state = 'active' AND lease_expires_at < UTC_TIMESTAMP(6)
ORDER BY lease_expires_at
LIMIT 100
FOR UPDATE SKIP LOCKED;
`;

export const SWEEP_RETRY = `
UPDATE jobs
SET state = 'available',
    run_at = UTC_TIMESTAMP(6) + INTERVAL
      LEAST(
        86400,
        IF(retry_backoff = 1,
           retry_delay_secs * POW(2, retry_count),
           retry_delay_secs)
        + FLOOR(RAND() * retry_delay_secs)
      ) SECOND,
    retry_count = retry_count + 1,
    locked_by = NULL, lease_expires_at = NULL, started_at = NULL,
    last_error = JSON_OBJECT('message', 'lease expired', 'at', UTC_TIMESTAMP(6))
WHERE id IN (?) AND retry_count < retry_limit;
`;

export const SWEEP_DLQ_INSERT = `
INSERT INTO jobs_dead
  (id, queue, priority, payload, singleton_key,
   retry_count, retry_limit, retry_delay_secs, retry_backoff,
   created_at, last_error)
SELECT id, queue, priority, payload, singleton_key,
       retry_count + 1, retry_limit, retry_delay_secs, retry_backoff,
       created_at, JSON_OBJECT('message', 'lease expired', 'at', UTC_TIMESTAMP(6))
FROM jobs
WHERE id IN (?);
`;

export const SWEEP_DLQ_DELETE = `
DELETE FROM jobs WHERE id IN (?);
`;

// §6.3 — Archive retention

export const ARCHIVE_PRUNE = `
DELETE FROM jobs_archive
WHERE completed_at < UTC_TIMESTAMP(6) - INTERVAL ? DAY
ORDER BY completed_at
LIMIT 5000;
`;

// Archive query

export const GET_ARCHIVED_JOB = `
SELECT CAST(id AS CHAR) AS id, queue, priority, payload, singleton_key, retry_count,
       UNIX_TIMESTAMP(created_at) AS created_at_unix,
       UNIX_TIMESTAMP(started_at) AS started_at_unix,
       UNIX_TIMESTAMP(completed_at) AS completed_at_unix,
       duration_ms
FROM jobs_archive
WHERE id = ?;
`;

export const LIST_ARCHIVE = `
SELECT CAST(id AS CHAR) AS id, queue, priority, payload, singleton_key, retry_count,
       UNIX_TIMESTAMP(created_at) AS created_at_unix,
       UNIX_TIMESTAMP(started_at) AS started_at_unix,
       UNIX_TIMESTAMP(completed_at) AS completed_at_unix,
       duration_ms
FROM jobs_archive
WHERE queue = ? AND completed_at < ?
ORDER BY completed_at DESC
LIMIT ?;
`;

// Connection setup

export const INIT_SESSION = `
SET SESSION transaction_isolation = 'READ-COMMITTED',
            SESSION time_zone = '+00:00';
`;
