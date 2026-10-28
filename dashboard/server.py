#!/usr/bin/env python3
"""Live pipeline command-center dashboard.

Real-time visualization of the streaming anomaly-detection pipeline:
    producer -> kafka -> faust -> sinks -> minio / timescaledb / airflow

Run from this directory:
    python server.py

Then open http://localhost:8082
"""

import base64
import json
import queue
import threading
import time
import urllib.request
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import boto3
import psycopg2
from kafka import KafkaConsumer, TopicPartition
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

# ─── CONFIG ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
LIVE_TOPICS = ["raw-metrics", "normal-events", "anomalies"]
SINK_GROUPS = ["anomaly-sink", "normal-sink"]
DAG_IDS = ["etl_pipeline", "data_quality_check", "anomaly_report"]

PG = dict(host="localhost", port=5433, dbname="metrics_db",
          user="pipeline_user", password="pipeline_pass")
PG_TABLES = ["raw_metrics", "anomaly_events", "etl_metric_summaries",
             "data_quality_log", "anomaly_reports"]

MINIO = dict(endpoint="http://localhost:9000",
             access="minio_admin", secret="minio_password")
MINIO_BUCKETS = ["normal-events", "etl-processed"]

AIRFLOW_API = "http://localhost:8081/api/v1"
AIRFLOW_AUTH = ("admin", "admin")

PORT = 8082
STATIC_DIR = Path(__file__).parent / "static"
SNAPSHOT_INTERVAL = 2.0

# ─── PROMETHEUS METRICS ───────────────────────────────────────
DASHBOARD_SNAPSHOTS = Counter(
    "dashboard_snapshots_total",
    "Total state snapshots broadcast to SSE clients",
)
DASHBOARD_EVENTS_BROADCAST = Counter(
    "dashboard_events_broadcast_total",
    "Total live events broadcast to SSE clients",
    ["kind"],
)
DASHBOARD_CLIENTS = Gauge(
    "dashboard_sse_clients",
    "Number of connected SSE clients",
)
DASHBOARD_API_ERRORS = Counter(
    "dashboard_api_errors_total",
    "Total errors fetching from Kafka / TimescaleDB / MinIO / Airflow",
    ["source"],
)

# Precision metrics
DASHBOARD_SNAPSHOT_DURATION_SECONDS = Histogram(
    "dashboard_snapshot_duration_seconds",
    "Time to collect and broadcast a full state snapshot (seconds)",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0),
)
DASHBOARD_EVENT_END_TO_END_LATENCY_SECONDS = Histogram(
    "dashboard_event_end_to_end_latency_seconds",
    "Time from event creation (event_timestamp) to dashboard broadcast (seconds)",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0),
)
DASHBOARD_SSE_CONNECTS = Counter(
    "dashboard_sse_connects_total",
    "Total SSE client connections",
)
DASHBOARD_SSE_DISCONNECTS = Counter(
    "dashboard_sse_disconnects_total",
    "Total SSE client disconnections",
)

# ─── SHARED STATE ──────────────────────────────────────────────────
clients = set()
clients_lock = threading.Lock()
broadcast_queue = queue.Queue(maxsize=5000)

state_lock = threading.Lock()
window = {"ts": time.time(), "raw": 0, "normal": 0, "anomaly": 0}


def bcast(msg):
    try:
        broadcast_queue.put_nowait(msg)
    except queue.Full:
        pass


# ─── COLLECTORS ────────────────────────────────────────────────────
def live_consumer_thread():
    """Subscribe to live topics and forward events to the UI."""
    consumer = None
    while True:
        try:
            if consumer is None:
                consumer = KafkaConsumer(
                    *LIVE_TOPICS,
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    group_id="dashboard-live",
                    enable_auto_commit=False,
                    auto_offset_reset="latest",
                    consumer_timeout_ms=400,
                )
            records = consumer.poll(timeout_ms=500, max_records=200)
            if not records:
                continue
            with state_lock:
                for tps, recs in records.items():
                    for rec in recs:
                        v = json.loads(rec.value.decode("utf-8"))
                        kind = "raw"
                        event_ts_str = v.get("event_timestamp")
                        if rec.topic == "raw-metrics":
                            window["raw"] += 1
                            bcast({"type": "event", "kind": "raw",
                                   "metric": v.get("metric_name"),
                                   "host": v.get("host"),
                                   "value": v.get("value"),
                                   "ts": v.get("event_timestamp")})
                        elif rec.topic == "normal-events":
                            window["normal"] += 1
                            kind = "normal"
                        elif rec.topic == "anomalies":
                            window["anomaly"] += 1
                            kind = "anomaly"
                            bcast({"type": "event", "kind": "anomaly",
                                   "metric": v.get("metric_name"),
                                   "host": v.get("host"),
                                   "value": v.get("value"),
                                   "z": v.get("deviation_score"),
                                   "ts": v.get("event_timestamp")})
                        DASHBOARD_EVENTS_BROADCAST.labels(kind=kind).inc()

                        # end-to-end latency: event creation → dashboard broadcast
                        if event_ts_str:
                            try:
                                event_dt = datetime.fromisoformat(
                                    event_ts_str.replace("Z", "+00:00")
                                )
                                latency = (
                                    datetime.now(timezone.utc) - event_dt
                                ).total_seconds()
                                if latency >= 0:
                                    DASHBOARD_EVENT_END_TO_END_LATENCY_SECONDS.observe(
                                        latency
                                    )
                            except Exception:
                                pass
        except Exception as ex:
            DASHBOARD_API_ERRORS.labels(source="kafka").inc()
            bcast({"type": "log", "msg": f"live consumer: {ex}"})
            time.sleep(2)


def kafka_offsets():
    """End offsets per topic."""
    try:
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP,
                                 group_id="dashboard-offsets",
                                 enable_auto_commit=False)
        consumer.poll(timeout_ms=1000, max_records=1)
        offsets = {}
        for topic in LIVE_TOPICS:
            parts = consumer.partitions_for_topic(topic)
            if not parts:
                continue
            tps = [TopicPartition(topic, p) for p in parts]
            offs = consumer.end_offsets(tps)
            offsets[topic] = sum(offs.values())
        consumer.close()
        return offsets
    except Exception as ex:
        DASHBOARD_API_ERRORS.labels(source="kafka").inc()
        return {"error": str(ex)}


def kafka_group_lag(offsets):
    """Per-group committed offset lag for sink consumer groups."""
    try:
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP,
                                 client_id="dashboard-admin")
        lag = {}
        for group in SINK_GROUPS:
            res = admin.list_group_offsets(group)
            committed = res.get(group, {})
            total = 0
            for tp, om in committed.items():
                end = offsets.get(tp.topic)
                if end is not None:
                    total += max(0, end - om.offset)
            lag[group] = total
        admin.close()
        return lag
    except Exception as ex:
        DASHBOARD_API_ERRORS.labels(source="kafka").inc()
        return {"error": str(ex)}


def db_counts():
    """TimescaleDB row counts + most recent anomalies."""
    try:
        conn = psycopg2.connect(**PG)
        cur = conn.cursor()
        counts = {}
        for t in PG_TABLES:
            cur.execute(f"SELECT count(*) FROM {t}")
            counts[t] = cur.fetchone()[0]
        cur.execute("""
            SELECT time, metric_name, host, value, deviation_score
            FROM anomaly_events ORDER BY time DESC LIMIT 12
        """)
        recent = [{"time": r[0].isoformat(), "metric": r[1], "host": r[2],
                   "value": round(float(r[3]), 2), "z": round(float(r[4]), 2)}
                  for r in cur.fetchall()]
        conn.close()
        return counts, recent
    except Exception as ex:
        DASHBOARD_API_ERRORS.labels(source="timescaledb").inc()
        return {"error": str(ex)}, []


def minio_counts():
    """Object counts per bucket (capped at 1000 for speed)."""
    try:
        s3 = boto3.client("s3", endpoint_url=MINIO["endpoint"],
                          aws_access_key_id=MINIO["access"],
                          aws_secret_access_key=MINIO["secret"])
        out = {}
        for bucket in MINIO_BUCKETS:
            n, truncated = 0, False
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket,
                                           PaginationConfig={"MaxItems": 1000,
                                                             "PageSize": 100}):
                n += len(page.get("Contents", []))
                truncated = page.get("IsTruncated", False)
                if n >= 1000:
                    break
            out[bucket] = {"count": n, "truncated": truncated}
        return out
    except Exception as ex:
        DASHBOARD_API_ERRORS.labels(source="minio").inc()
        return {"error": str(ex)}


def airflow_dag_states():
    """Latest run state per DAG via the Airflow REST API."""
    token = base64.b64encode(f"{AIRFLOW_AUTH[0]}:{AIRFLOW_AUTH[1]}".encode()).decode()
    states = {}
    for dag in DAG_IDS:
        try:
            req = urllib.request.Request(
                f"{AIRFLOW_API}/dags/{dag}/dagRuns?limit=1&order_by=-start_date")
            req.add_header("Authorization", f"Basic {token}")
            with urllib.request.urlopen(req, timeout=3) as r:
                data = json.loads(r.read())
            runs = data.get("dag_runs", [])
            states[dag] = runs[0]["state"] if runs else "none"
        except Exception:
            DASHBOARD_API_ERRORS.labels(source="airflow").inc()
            states[dag] = "unreachable"
    return states


def snapshot_thread():
    """Periodically compose a full state snapshot and broadcast it."""
    while True:
        time.sleep(SNAPSHOT_INTERVAL)
        start = time.perf_counter()
        try:
            with state_lock:
                raw, normal, anomaly = window["raw"], window["normal"], window["anomaly"]
                window.update(raw=0, normal=0, anomaly=0)

            offsets = kafka_offsets()
            lag = kafka_group_lag(offsets)
            db_counts_res, recent_anomalies = db_counts()
            minio = minio_counts()
            airflow = airflow_dag_states()

            DASHBOARD_SNAPSHOTS.inc()
            snapshot_duration = time.perf_counter() - start
            DASHBOARD_SNAPSHOT_DURATION_SECONDS.observe(snapshot_duration)
            bcast({
                "type": "snapshot",
                "ts": datetime.now(timezone.utc).isoformat(),
                "rates": {
                    "raw": raw,
                    "normal": normal,
                    "anomaly": anomaly,
                    "total": raw + normal + anomaly,
                },
                "kafka": {"offsets": offsets, "lag": lag},
                "db": db_counts_res,
                "minio": minio,
                "airflow": airflow,
                "anomalies_recent": recent_anomalies,
            })
        except Exception as ex:
            bcast({"type": "log", "msg": f"snapshot: {ex}"})


def broadcaster():
    """Fan a single message out to every connected SSE client."""
    while True:
        msg = broadcast_queue.get()
        with clients_lock:
            stale = []
            for q in list(clients):
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    stale.append(q)
            for q in stale:
                clients.discard(q)
            DASHBOARD_CLIENTS.set(len(clients))


# ─── HTTP ──────────────────────────────────────────────────────────
class QuietThreadingHTTPServer(ThreadingHTTPServer):
    """HTTPServer that ignores noisy client-disconnect errors."""

    def handle_error(self, request, client_address):
        import sys
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, BrokenPipeError)):
            return
        super().handle_error(request, client_address)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        if self.path.startswith("/api/events"):
            self.stream_events()
        elif self.path == "/metrics":
            self.stream_metrics()
        else:
            super().do_GET()

    def stream_metrics(self):
        """Expose Prometheus text-format metrics."""
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPE_LATEST)
        self.end_headers()
        self.wfile.write(generate_latest())

    def stream_events(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        q = queue.Queue(maxsize=200)
        DASHBOARD_SSE_CONNECTS.inc()
        with clients_lock:
            clients.add(q)
        try:
            while True:
                try:
                    msg = q.get(timeout=15)
                    self.wfile.write(f"data: {json.dumps(msg)}\n\n".encode())
                    self.wfile.flush()
                except queue.Empty:
                    self.wfile.write(b": keep-alive\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            DASHBOARD_SSE_DISCONNECTS.inc()
            with clients_lock:
                clients.discard(q)


def main():
    for name, fn in (("live", live_consumer_thread),
                     ("snapshot", snapshot_thread),
                     ("broadcaster", broadcaster)):
        t = threading.Thread(target=fn, name=name, daemon=True)
        t.start()

    server = QuietThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"📊 Command center running at http://localhost:{PORT}")
    print(f"   (producer + sinks must be running for live data)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down")


if __name__ == "__main__":
    main()
