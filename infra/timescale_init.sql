-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- TABLE 1: raw_metrics
CREATE TABLE IF NOT EXISTS raw_metrics (
    time            TIMESTAMPTZ         NOT NULL,
    metric_name     TEXT                NOT NULL,
    host            TEXT                NOT NULL,
    value           DOUBLE PRECISION    NOT NULL,
    is_anomaly      BOOLEAN             DEFAULT FALSE
);
SELECT create_hypertable('raw_metrics', 'time', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_raw_metrics_name_time ON raw_metrics (metric_name, time DESC);

-- TABLE 2: anomaly_events
CREATE TABLE IF NOT EXISTS anomaly_events (
    time                TIMESTAMPTZ         NOT NULL,
    metric_name         TEXT                NOT NULL,
    host                TEXT                NOT NULL,
    value               DOUBLE PRECISION    NOT NULL,
    baseline_mean       DOUBLE PRECISION    NOT NULL,
    baseline_stddev     DOUBLE PRECISION    NOT NULL,
    deviation_score     DOUBLE PRECISION    NOT NULL,
    threshold           DOUBLE PRECISION    NOT NULL,
    window_start        TIMESTAMPTZ         NOT NULL,
    window_end          TIMESTAMPTZ         NOT NULL,
    is_late_arrival     BOOLEAN             DEFAULT FALSE
);
SELECT create_hypertable('anomaly_events', 'time', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_anomaly_events_metric_time ON anomaly_events (metric_name, time DESC);

-- TABLE 3: etl_metric_summaries (written by Airflow DAG 1)
CREATE TABLE IF NOT EXISTS etl_metric_summaries (
    time            TIMESTAMPTZ         NOT NULL,
    metric_name     TEXT                NOT NULL,
    host            TEXT                NOT NULL,
    avg_value       DOUBLE PRECISION,
    min_value       DOUBLE PRECISION,
    max_value       DOUBLE PRECISION,
    stddev_value    DOUBLE PRECISION,
    event_count     INTEGER,
    anomaly_count   INTEGER             DEFAULT 0,
    processed_at    TIMESTAMPTZ         DEFAULT NOW()
);
SELECT create_hypertable('etl_metric_summaries', 'time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- TABLE 4: data_quality_log (written by Airflow DAG 2)
CREATE TABLE IF NOT EXISTS data_quality_log (
    id              SERIAL              PRIMARY KEY,
    checked_at      TIMESTAMPTZ         DEFAULT NOW(),
    file_path       TEXT                NOT NULL,
    row_count       INTEGER,
    null_count      INTEGER,
    schema_valid    BOOLEAN,
    value_range_ok  BOOLEAN,
    issues          TEXT,
    passed          BOOLEAN             NOT NULL
);

-- TABLE 5: anomaly_reports (written by Airflow DAG 3)
CREATE TABLE IF NOT EXISTS anomaly_reports (
    report_time         TIMESTAMPTZ     DEFAULT NOW(),
    metric_name         TEXT            NOT NULL,
    anomaly_count       INTEGER         DEFAULT 0,
    avg_deviation       DOUBLE PRECISION,
    max_deviation       DOUBLE PRECISION,
    anomaly_rate_pct    DOUBLE PRECISION,
    worst_host          TEXT,
    report_window_hrs   INTEGER         DEFAULT 1
);
SELECT create_hypertable('anomaly_reports', 'report_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- CONTINUOUS AGGREGATE: 5-min anomaly rate
CREATE MATERIALIZED VIEW IF NOT EXISTS anomaly_rate_5min
WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('5 minutes', time)  AS bucket,
        metric_name,
        COUNT(*)                        AS anomaly_count,
        AVG(deviation_score)            AS avg_deviation,
        MAX(deviation_score)            AS max_deviation
    FROM anomaly_events
    GROUP BY bucket, metric_name
WITH NO DATA;

SELECT add_continuous_aggregate_policy('anomaly_rate_5min',
    start_offset => INTERVAL '1 hour',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE);

-- RETENTION POLICIES
SELECT add_retention_policy('raw_metrics', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('anomaly_events', INTERVAL '30 days', if_not_exists => TRUE);