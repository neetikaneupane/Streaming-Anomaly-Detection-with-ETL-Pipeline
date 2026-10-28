import json
import threading
import time
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
from prometheus_client import start_http_server, Counter, Histogram, Gauge

# ─── CONFIG ───────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TIMESCALE_CONN = {
    "host": "localhost",
    "port": 5433,
    "dbname": "metrics_db",
    "user": "pipeline_user",
    "password": "pipeline_pass",
}
PROMETHEUS_PORT = 9101

# ─── PROMETHEUS METRICS ───────────────────────────────────────
ANOMALY_SINK_CONSUMED = Counter(
    "anomaly_sink_consumed_total",
    "Total anomaly events consumed from Kafka",
)
ANOMALY_SINK_INSERTED = Counter(
    "anomaly_sink_inserted_total",
    "Total anomaly events inserted into TimescaleDB",
)
ANOMALY_SINK_ERRORS = Counter(
    "anomaly_sink_errors_total",
    "Total errors in anomaly sink",
    ["error_type"],
)

# Precision metrics
ANOMALY_SINK_CONSUMED_BY_METRIC = Counter(
    "anomaly_sink_consumed_by_metric_total",
    "Anomaly events consumed from Kafka, broken down by metric:host",
    ["metric_name", "host"],
)
ANOMALY_SINK_DB_LATENCY_SECONDS = Histogram(
    "anomaly_sink_db_latency_seconds",
    "Time to commit a batch of anomalies to TimescaleDB (seconds)",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)
ANOMALY_SINK_BATCH_SIZE = Histogram(
    "anomaly_sink_batch_size",
    "Number of anomaly events per DB commit batch",
    buckets=(1, 5, 10, 25, 50, 100),
)
ANOMALY_SINK_PENDING_BATCH = Gauge(
    "anomaly_sink_pending_batch",
    "Number of events currently waiting in the batch buffer",
)

# Start Prometheus metrics HTTP server on port 9101 (daemon thread)
threading.Thread(target=start_http_server, args=(PROMETHEUS_PORT,), daemon=True).start()


def get_db():
    return psycopg2.connect(**TIMESCALE_CONN)

def insert_anomaly(cur, event: dict):
    cur.execute("""
        INSERT INTO anomaly_events (
            time, metric_name, host, value,
            baseline_mean, baseline_stddev, deviation_score,
            threshold, window_start, window_end, is_late_arrival
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        event.get("detected_at", datetime.utcnow().isoformat()),
        event["metric_name"],
        event["host"],
        event["value"],
        event["baseline_mean"],
        event["baseline_stddev"],
        event["deviation_score"],
        event["threshold"],
        event["window_start"],
        event["window_end"],
        event.get("is_late_arrival", False),
    ))

    # also write to raw_metrics with is_anomaly=True
    cur.execute("""
        INSERT INTO raw_metrics (time, metric_name, host, value, is_anomaly)
        VALUES (%s, %s, %s, %s, TRUE)
    """, (
        event.get("event_timestamp", datetime.utcnow().isoformat()),
        event["metric_name"],
        event["host"],
        event["value"],
    ))

def main():
    print("Starting anomaly sink → TimescaleDB")
    conn = get_db()
    conn.autocommit = False

    consumer = KafkaConsumer(
        "anomalies",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="anomaly-sink",
        auto_offset_reset="earliest",
    )

    print("Consuming anomalies topic...")
    batch = []
    BATCH_SIZE = 10

    for message in consumer:
        event = json.loads(message.value.decode("utf-8"))
        batch.append(event)
        ANOMALY_SINK_CONSUMED.inc()
        ANOMALY_SINK_CONSUMED_BY_METRIC.labels(
            metric_name=event["metric_name"], host=event["host"]
        ).inc()
        ANOMALY_SINK_PENDING_BATCH.set(len(batch))
        print(f"🚨 [{event['metric_name']}:{event['host']}] "
              f"value={event['value']:.4f} z={event['deviation_score']:.4f}")

        if len(batch) >= BATCH_SIZE:
            start = time.perf_counter()
            try:
                cur = conn.cursor()
                for e in batch:
                    insert_anomaly(cur, e)
                conn.commit()
                db_latency = time.perf_counter() - start
                ANOMALY_SINK_DB_LATENCY_SECONDS.observe(db_latency)
                ANOMALY_SINK_BATCH_SIZE.observe(len(batch))
                ANOMALY_SINK_INSERTED.inc(len(batch))
                print(f"✅ Committed {len(batch)} anomalies to TimescaleDB ({db_latency:.4f}s)")
                batch = []
                ANOMALY_SINK_PENDING_BATCH.set(0)
            except Exception as ex:
                conn.rollback()
                ANOMALY_SINK_ERRORS.labels(error_type=type(ex).__name__).inc()
                print(f"[ERROR] DB insert failed: {ex}")

if __name__ == "__main__":
    main()