import json
import threading
import time
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from kafka import KafkaConsumer
from datetime import datetime, timezone
from prometheus_client import start_http_server, Counter, Histogram, Gauge

# ─── CONFIG ───────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
MINIO_ENDPOINT  = "http://localhost:9000"
MINIO_ACCESS    = "minio_admin"
MINIO_SECRET    = "minio_password"
BUCKET          = "normal-events"
BATCH_SIZE      = 50       # write parquet every 50 events
FLUSH_INTERVAL  = 60       # or every 60 seconds
PROMETHEUS_PORT = 9102

# ─── PROMETHEUS METRICS ───────────────────────────────────────
NORMAL_SINK_CONSUMED = Counter(
    "normal_sink_consumed_total",
    "Total normal events consumed from Kafka",
)
NORMAL_SINK_PARQUET_WRITES = Counter(
    "normal_sink_parquet_writes_total",
    "Total Parquet files written to MinIO",
)
NORMAL_SINK_RECORDS_WRITTEN = Counter(
    "normal_sink_records_written_total",
    "Total records written to MinIO Parquet",
)
NORMAL_SINK_ERRORS = Counter(
    "normal_sink_errors_total",
    "Total errors in normal sink",
    ["error_type"],
)

# Precision metrics
NORMAL_SINK_CONSUMED_BY_METRIC = Counter(
    "normal_sink_consumed_by_metric_total",
    "Normal events consumed from Kafka, broken down by metric:host",
    ["metric_name", "host"],
)
NORMAL_SINK_PARQUET_LATENCY_SECONDS = Histogram(
    "normal_sink_parquet_latency_seconds",
    "Time to write a Parquet batch to MinIO (seconds)",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0),
)
NORMAL_SINK_BATCH_SIZE = Histogram(
    "normal_sink_batch_size",
    "Number of normal events per Parquet write batch",
    buckets=(1, 10, 25, 50, 100, 250, 500),
)
NORMAL_SINK_PENDING_BATCH = Gauge(
    "normal_sink_pending_batch",
    "Number of events currently waiting in the batch buffer",
)

# Start Prometheus metrics HTTP server on port 9102 (daemon thread)
threading.Thread(target=start_http_server, args=(PROMETHEUS_PORT,), daemon=True).start()

def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )

def write_parquet(s3, records: list):
    df = pd.DataFrame(records)
    df["ingest_time"] = datetime.now(timezone.utc).isoformat()

    # partition by date/hour
    now = datetime.now(timezone.utc)
    key = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/{now.strftime('%H%M%S%f')}.parquet"

    buf = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buf)
    buf.seek(0)

    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"✅ Wrote {len(records)} records to s3://{BUCKET}/{key}")

def main():
    print("Starting normal events sink → MinIO (Parquet)")
    s3 = get_s3()

    consumer = KafkaConsumer(
        "normal-events",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="normal-sink",
        auto_offset_reset="earliest",
    )

    batch = []
    last_flush = datetime.now(timezone.utc)

    print("Consuming normal-events topic...")
    for message in consumer:
        event = json.loads(message.value.decode("utf-8"))
        batch.append(event)
        NORMAL_SINK_CONSUMED.inc()
        NORMAL_SINK_CONSUMED_BY_METRIC.labels(
            metric_name=event["metric_name"], host=event["host"]
        ).inc()
        NORMAL_SINK_PENDING_BATCH.set(len(batch))

        now = datetime.now(timezone.utc)
        elapsed = (now - last_flush).total_seconds()

        if len(batch) >= BATCH_SIZE or elapsed >= FLUSH_INTERVAL:
            start = time.perf_counter()
            try:
                write_parquet(s3, batch)
                write_latency = time.perf_counter() - start
                NORMAL_SINK_PARQUET_LATENCY_SECONDS.observe(write_latency)
                NORMAL_SINK_BATCH_SIZE.observe(len(batch))
                NORMAL_SINK_PARQUET_WRITES.inc()
                NORMAL_SINK_RECORDS_WRITTEN.inc(len(batch))
                print(f"✅ Wrote {len(batch)} records to MinIO Parquet ({write_latency:.4f}s)")
                batch = []
                last_flush = now
                NORMAL_SINK_PENDING_BATCH.set(0)
            except Exception as ex:
                NORMAL_SINK_ERRORS.labels(error_type=type(ex).__name__).inc()
                print(f"[ERROR] Parquet write failed: {ex}")

if __name__ == "__main__":
    main()