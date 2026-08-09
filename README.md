# Streaming Anomaly Detection with ETL Pipeline

A real-time streaming pipeline that ingests synthetic metrics, detects anomalies in-stream using z-score over a rolling window baseline, and feeds the results into a lakehouse/warehouse-style ETL layer backed by Kafka, Faust, MinIO (S3), Airflow, and TimescaleDB.

## Architecture

```
                         ┌────────────────────────────────────────────┐
                         │                 Kafka UI                   │  :8080
                         └───────────────────┬────────────────────────┘
                                             │
┌─────────────┐   raw-metrics   ┌────────────▼───────────┐
│  Producer   │ ─────────────► │        Kafka           │
│ (synthetic  │                 │  raw-metrics           │
│  metrics)   │                 │  anomalies             │
└─────────────┘                 │  normal-events         │
                                └───┬───────────┬────────┘
                                    │           │
                     normal-events  │           │  anomalies
                                    ▼           ▼
                           ┌────────────┐  ┌──────────────┐
                           │ Faust      │  │ anomaly_sink │
                           │ worker     │  │  (py script) │
                           │ (RocksDB   │  └──────┬───────┘
                           │  state)    │         │
                           └──────┬─────┘         ▼
                                  │          TimescaleDB
                                  │        anomaly_events
                                  ▼
                        ┌─────────────────┐
                        │  normal_sink    │
                        │ (py script)     │
                        └────────┬────────┘
                                 ▼
                          MinIO (S3)
                         normal-events
                           *.parquet
                                 │
                                 ▼
                        ┌─────────────────┐    etl-processed / archived
                        │  Airflow DAGs   │ ────────────────────────────►
                        │  - etl_pipeline │
                        │  - data_quality │    TimescaleDB
                        │  - anomaly_report│  etl_metric_summaries
                        └─────────────────┘  data_quality_log
                                             anomaly_reports
```

### Flow
1. **Producer** (`producer/producer.py`) emits synthetic metrics (`cpu_usage`, `api_latency`, `order_volume`) across 5 hosts with daily seasonality, Gaussian noise, injected spikes, and ~5% late arrivals.
2. **Faust worker** (`faust_app/app.py`) consumes `raw-metrics`, maintains per `metric:host` windowed statistics in a RocksDB-backed state store, builds a weighted baseline from the last 10 windows, and flags events with `|z-score| > 3.0` as anomalies.
3. **normal_sink** (`sinks/normal_sink.py`) writes non-anomalous events to MinIO as partitioned Parquet files.
4. **anomaly_sink** (`sinks/anomaly_sink.py`) persists anomalous events to TimescaleDB (`anomaly_events` + `raw_metrics` flag).
5. **Airflow DAGs** run on a schedule:
   - `etl_pipeline` (every 15 min) — extract Parquet from MinIO → clean/aggregate into 15-min summaries → load into `etl_metric_summaries` → archive processed files.
   - `data_quality_check` (every 30 min) — schema, null-rate, and value-range checks logged to `data_quality_log`.
   - `anomaly_report` (hourly) — rolls up anomalies into `anomaly_reports`.

## Prerequisites

- Docker + Docker Compose
- Python 3.11 (for running the producer/sink scripts on your Mac)

## Quick Start

### 1. Start the infrastructure

```bash
docker compose up -d --build
```

This brings up Zookeeper, Kafka, Kafka UI, MinIO, TimescaleDB, Airflow (webserver + scheduler), and the Faust worker. Topic creation, bucket creation, and the TimescaleDB schema are handled by one-shot init containers.

### 2. Verify health

```bash
docker compose ps          # all services Up, init containers Exited (0)
docker logs -f faust-worker  # should show "Worker ready" then processing events
```

### 3. Produce metrics and run the sinks (from your Mac)

```bash
# terminal 1 — generate data
python producer/producer.py

# terminal 2 — write normal events to MinIO (Parquet)
python sinks/normal_sink.py

# terminal 3 — write anomalies to TimescaleDB
python sinks/anomaly_sink.py
```

> If you use a virtualenv, activate it first: `source .venv311/bin/activate`

### 4. Trigger the ETL

```bash
docker exec airflow-webserver airflow dags trigger etl_pipeline
docker exec airflow-webserver airflow dags trigger data_quality_check
docker exec airflow-webserver airflow dags trigger anomaly_report
```

## Live Dashboard

A self-contained **command-center dashboard** (`dashboard/`) that visualizes the whole pipeline in real time — an animated flow diagram with data particles traveling through each stage, live metric charts, an anomaly feed, Kafka lag, TimescaleDB/MinIO counts, and Airflow DAG status.

![dashboard](https://img.shields.io/badge/status-live-brightgreen)

```bash
python dashboard/server.py        # serves http://localhost:8082
```

Open http://localhost:8082 while the producer (and optionally the sinks) are running. The dashboard is stdlib-only on the backend and dependency-free on the frontend (vanilla canvas + Server-Sent Events) — no npm, no build step.

It pulls live data from:
- **Kafka** — subscribes to `raw-metrics` / `anomalies` / `normal-events` and streams each event to the browser; also reads topic end-offsets and sink consumer-group lag.
- **TimescaleDB** — row counts per table and the most recent anomalies (`localhost:5433`).
- **MinIO** — object counts for `normal-events` and `etl-processed`.
- **Airflow REST API** — latest run state for the three DAGs (requires the API basic-auth backend enabled on the webserver, already set in `docker-compose.yml`).

## Ports & Credentials

| Service      | Port | URL / Credentials                              |
|--------------|------|------------------------------------------------|
| Kafka UI     | 8080 | http://localhost:8080                          |
| Airflow      | 8081 | http://localhost:8081 — `admin` / `admin`      |
| Dashboard    | 8082 | http://localhost:8082                          |
| MinIO API    | 9000 | `minio_admin` / `minio_password`               |
| MinIO Console| 9001 | http://localhost:9001                          |
| Kafka broker | 9092 | `localhost:9092`                               |
| TimescaleDB  | 5433 | `pipeline_user` / `pipeline_pass`, db `metrics_db` |

> **Note:** TimescaleDB is exposed on host port **5433** (not 5432) to avoid conflicting with a local Homebrew PostgreSQL. Inside the Docker network it is reached as `timescaledb:5432`. The Airflow DAGs and the Faust worker already use the internal hostname; only host-side scripts (e.g. `sinks/anomaly_sink.py`) connect via `localhost:5433`.

## Storage Layout

**Kafka topics:** `raw-metrics`, `anomalies`, `normal-events`

**MinIO buckets:**
- `normal-events` — incoming Parquet files (partitioned `year=…/month=…/day=…/hour=…`)
- `etl-processed` — archived Parquet after ETL processing
- `anomaly-reports` — (reserved)

**TimescaleDB tables (`metrics_db`):**
- `raw_metrics` — hypertable of all metrics, `is_anomaly` flag
- `anomaly_events` — hypertable of detected anomalies with baseline/z-score context
- `etl_metric_summaries` — 15-min aggregates written by the ETL DAG
- `data_quality_log` — quality check results
- `anomaly_reports` — hourly anomaly rollups
- `anomaly_rate_5min` — continuous aggregate over `anomaly_events`

## Project Layout

```
├── producer/producer.py        # synthetic metric generator
├── faust_app/                  # Faust stream processor (RocksDB state)
│   ├── app.py
│   └── Dockerfile
├── sinks/
│   ├── anomaly_sink.py         # anomalies → TimescaleDB
│   └── normal_sink.py          # normal events → MinIO Parquet
├── dashboard/                  # live pipeline visualization
│   ├── server.py               # stdlib HTTP + SSE backend
│   └── static/                 # HTML/CSS/JS frontend (no deps)
├── airflow/dags/               # Airflow DAGs (ETL, quality, reporting)
├── infra/timescale_init.sql    # TimescaleDB schema + policies
├── monitoring/                 # (reserved)
└── docker-compose.yml          # full stack definition
```

## Configuration Notes

- Anomaly detection: 60s windows, 30s grace for late arrivals, baseline over last 10 windows, z-score threshold `3.0` — all configurable in `faust_app/app.py`.
- The Faust worker persists its state to the `rocksdb_data` volume (`anomaly-detector-data`), so it recovers window history across restarts.
