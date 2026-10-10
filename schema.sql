-- =====================================================================
-- SAP Alert Platform - PostgreSQL + TimescaleDB schema (v2)
-- Run on a fresh database:  psql -d <db_name> -f schema.sql
-- =====================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- 1. groups
-- ---------------------------------------------------------------------
CREATE TABLE groups (
    id                        SERIAL PRIMARY KEY,
    name                      VARCHAR(100) NOT NULL UNIQUE,
    teams_webhook_vault_path  VARCHAR(200),             -- vault path, not the URL itself
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- 2. customers
-- ---------------------------------------------------------------------
CREATE TABLE customers (
    id               SERIAL PRIMARY KEY,
    group_id         INT NOT NULL REFERENCES groups(id),
    customer_number  VARCHAR(50) NOT NULL UNIQUE,
    alias            VARCHAR(50),
    name             VARCHAR(200) NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- 3. systems
-- ---------------------------------------------------------------------
CREATE TABLE systems (
    id               SERIAL PRIMARY KEY,
    customer_id      INT NOT NULL REFERENCES customers(id),
    sid              VARCHAR(10) NOT NULL,
    alias            VARCHAR(50),
    system_type      VARCHAR(10) NOT NULL
                     CHECK (system_type IN ('PROD', 'QA', 'DEV')),
    deployment_type  VARCHAR(20) NOT NULL
                     CHECK (deployment_type IN ('on_prem', 'private_cloud', 'saas')),
    source           VARCHAR(20) NOT NULL
                     CHECK (source IN ('collector', 'cloud_alm')),
    vault_path       VARCHAR(200),                      -- credentials reference only
    config           JSONB,                             -- e.g. Cloud ALM tenant details
    is_active        BOOLEAN NOT NULL DEFAULT true,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_id, sid)
);

-- ---------------------------------------------------------------------
-- 4. metric_definitions (the list of valid metrics, with units)
-- ---------------------------------------------------------------------
CREATE TABLE metric_definitions (
    name         VARCHAR(100) PRIMARY KEY,
    unit         VARCHAR(20) NOT NULL,                  -- %, ms, count, status, minutes
    category     VARCHAR(20) NOT NULL
                 CHECK (category IN ('os', 'instance', 'application',
                                     'database', 'collector')),
    description  VARCHAR(200)
);

-- ---------------------------------------------------------------------
-- 5. metrics (time-series, narrow design)
-- ---------------------------------------------------------------------
CREATE TABLE metrics (
    system_id     INT NOT NULL REFERENCES systems(id),
    metric_name   VARCHAR(100) NOT NULL REFERENCES metric_definitions(name),
    value         DOUBLE PRECISION NOT NULL,
    collected_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (system_id, metric_name, collected_at)  -- blocks duplicate resends
);

SELECT create_hypertable('metrics', 'collected_at',
                         chunk_time_interval => INTERVAL '1 day');

CREATE INDEX idx_metrics_system_time ON metrics (system_id, collected_at DESC);

-- Compression: chunks older than 7 days are compressed automatically
ALTER TABLE metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'system_id, metric_name',
    timescaledb.compress_orderby   = 'collected_at DESC'
);
SELECT add_compression_policy('metrics', INTERVAL '7 days');

-- Continuous aggregates (hourly rollups) and retention policies: to be added later

-- ---------------------------------------------------------------------
-- 6. rules
-- ---------------------------------------------------------------------
CREATE TABLE rules (
    id                SERIAL PRIMARY KEY,
    customer_id       INT REFERENCES customers(id),     -- NULL = global default rule
    system_id         INT REFERENCES systems(id),       -- NULL = all customer systems
    metric_name       VARCHAR(100) NOT NULL REFERENCES metric_definitions(name),
    operator          VARCHAR(2) NOT NULL
                      CHECK (operator IN ('>', '<', '>=', '<=', '=')),
    threshold         DOUBLE PRECISION NOT NULL,
    duration_minutes  INT NOT NULL DEFAULT 0,
    alert_name        VARCHAR(200) NOT NULL,
    alert_type        VARCHAR(50) NOT NULL,
    severity          VARCHAR(10) NOT NULL
                      CHECK (severity IN ('critical', 'warning', 'info')),
    is_active         BOOLEAN NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- 7. alerts
-- status meanings:
--   open             active and notified
--   cleared          metric back to normal
--   flapping         metric keeps crossing the threshold
--   suspected_false  correlation says likely false (reason in status_reason)
--   suppressed       not notified, e.g. maintenance window (reason in status_reason)
-- "Notified" = the alert has at least one row in notifications.
-- ---------------------------------------------------------------------
CREATE TABLE alerts (
    id                BIGSERIAL PRIMARY KEY,
    system_id         INT NOT NULL REFERENCES systems(id),
    rule_id           INT NOT NULL REFERENCES rules(id),
    alert_name        VARCHAR(200) NOT NULL,            -- copied from rule at fire time
    alert_type        VARCHAR(50) NOT NULL,
    metric_name       VARCHAR(100) NOT NULL REFERENCES metric_definitions(name),
    metric_value      DOUBLE PRECISION,
    threshold_value   DOUBLE PRECISION,
    severity          VARCHAR(10) NOT NULL
                      CHECK (severity IN ('critical', 'warning', 'info')),
    status            VARCHAR(20) NOT NULL
                      CHECK (status IN ('open', 'cleared', 'flapping',
                                        'suspected_false', 'suppressed')),
    status_reason     TEXT,
    occurrence_count  INT NOT NULL DEFAULT 1,
    fired_at          TIMESTAMPTZ NOT NULL,
    last_seen_at      TIMESTAMPTZ NOT NULL,
    cleared_at        TIMESTAMPTZ,
    llm_guidance      TEXT
);

CREATE INDEX idx_alerts_dedup    ON alerts (system_id, rule_id, status);
CREATE INDEX idx_alerts_fired_at ON alerts (fired_at);
CREATE INDEX idx_alerts_open     ON alerts (severity, fired_at DESC)
                                 WHERE status = 'open';

-- ---------------------------------------------------------------------
-- 8. collectors (one collector per host, one SAP system per host)
-- ---------------------------------------------------------------------
CREATE TABLE collectors (
    id                 SERIAL PRIMARY KEY,
    system_id          INT NOT NULL REFERENCES systems(id),
    hostname           VARCHAR(100) NOT NULL UNIQUE,
    token_hash         VARCHAR(200) NOT NULL UNIQUE,
    config             JSONB,
    last_heartbeat_at  TIMESTAMPTZ,
    version            VARCHAR(20),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- 9. maintenance_windows
-- ---------------------------------------------------------------------
CREATE TABLE maintenance_windows (
    id         SERIAL PRIMARY KEY,
    system_id  INT NOT NULL REFERENCES systems(id),
    start_at   TIMESTAMPTZ NOT NULL,
    end_at     TIMESTAMPTZ NOT NULL,
    reason     VARCHAR(200),
    CHECK (end_at > start_at)
);

-- ---------------------------------------------------------------------
-- 10. contacts
-- ---------------------------------------------------------------------
CREATE TABLE contacts (
    id            SERIAL PRIMARY KEY,
    group_id      INT REFERENCES groups(id),            -- consultants
    customer_id   INT REFERENCES customers(id),         -- ECS and customer contacts
    name          VARCHAR(100) NOT NULL,
    email         VARCHAR(200) NOT NULL,
    phone         VARCHAR(20),                          -- required for consultants
    contact_type  VARCHAR(20) NOT NULL
                  CHECK (contact_type IN ('consultant', 'ecs', 'customer')),
    is_active     BOOLEAN NOT NULL DEFAULT true,
    CHECK (
        (contact_type = 'consultant'
            AND group_id IS NOT NULL AND phone IS NOT NULL)
        OR
        (contact_type IN ('ecs', 'customer')
            AND customer_id IS NOT NULL)
    )
);

-- ---------------------------------------------------------------------
-- 11. oncall_schedules
-- ---------------------------------------------------------------------
CREATE TABLE oncall_schedules (
    id          SERIAL PRIMARY KEY,
    group_id    INT NOT NULL REFERENCES groups(id),
    contact_id  INT NOT NULL REFERENCES contacts(id),
    start_at    TIMESTAMPTZ NOT NULL,
    end_at      TIMESTAMPTZ NOT NULL,
    CHECK (end_at > start_at)
);

-- ---------------------------------------------------------------------
-- 12. notifications
-- ---------------------------------------------------------------------
CREATE TABLE notifications (
    id             BIGSERIAL PRIMARY KEY,
    alert_id       BIGINT NOT NULL REFERENCES alerts(id),
    contact_id     INT REFERENCES contacts(id),         -- email / sms / call
    group_id       INT REFERENCES groups(id),           -- teams channel
    channel        VARCHAR(10) NOT NULL
                   CHECK (channel IN ('email', 'sms', 'call', 'teams')),
    status         VARCHAR(10) NOT NULL
                   CHECK (status IN ('sent', 'delivered', 'failed')),
    error_message  TEXT,
    sent_at        TIMESTAMPTZ NOT NULL,
    CHECK (num_nonnulls(contact_id, group_id) = 1)      -- exactly one recipient type
);

CREATE INDEX idx_notifications_alert ON notifications (alert_id);

-- ---------------------------------------------------------------------
-- 13. users (only admins and management log in)
-- ---------------------------------------------------------------------
CREATE TABLE users (
    id          SERIAL PRIMARY KEY,
    email       VARCHAR(200) NOT NULL UNIQUE,
    role        VARCHAR(20) NOT NULL
                CHECK (role IN ('admin', 'management')),
    is_active   BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- 14. audit_log
-- ---------------------------------------------------------------------
CREATE TABLE audit_log (
    id          BIGSERIAL PRIMARY KEY,
    user_id     INT REFERENCES users(id),               -- NULL = platform action
    action      VARCHAR(50) NOT NULL,
    entity      VARCHAR(50) NOT NULL,
    entity_id   VARCHAR(50),
    details     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =====================================================================
-- Seed data
-- =====================================================================

-- Starter metric list (edit to match what the collectors actually send)
INSERT INTO metric_definitions (name, unit, category, description) VALUES
    ('cpu_usage_pct',            '%',       'os',          'Host CPU usage'),
    ('memory_usage_pct',         '%',       'os',          'Host memory usage'),
    ('filesystem_usage_pct',     '%',       'os',          'File system usage'),
    ('instance_available',       'status',  'instance',    '1 = ABAP instance up, 0 = down'),
    ('free_dialog_wp_pct',       '%',       'instance',    'Free dialog work processes'),
    ('system_available',         'status',  'application', '1 = ABAP system up, 0 = down'),
    ('dialog_response_time_ms',  'ms',      'application', 'Average dialog response time'),
    ('update_response_time_ms',  'ms',      'application', 'Average update response time'),
    ('abap_short_dumps_count',   'count',   'application', 'ABAP short dumps in the interval'),
    ('failed_batch_jobs_count',  'count',   'application', 'Failed batch jobs in the interval'),
    ('db_available',             'status',  'database',    '1 = database up, 0 = down'),
    ('db_replication_in_sync',   'status',  'database',    '1 = replication in sync, 0 = not'),
    ('heartbeat_age_minutes',    'minutes', 'collector',   'Minutes since the last collector heartbeat');

-- Built-in global rule for collector-down alerts
INSERT INTO rules (customer_id, system_id, metric_name, operator, threshold,
                   duration_minutes, alert_name, alert_type, severity)
VALUES (NULL, NULL, 'heartbeat_age_minutes', '>', 5,
        0, 'Collector heartbeat missing', 'Availability', 'critical');