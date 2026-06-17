import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from kafka import KafkaConsumer
from datetime import datetime, timezone

# ─── CONFIG ───────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
MINIO_ENDPOINT  = "http://localhost:9000"
MINIO_ACCESS    = "minio_admin"
MINIO_SECRET    = "minio_password"
BUCKET          = "normal-events"
BATCH_SIZE      = 50       # write parquet every 50 events
FLUSH_INTERVAL  = 60       # or every 60 seconds

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
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    batch = []
    last_flush = datetime.now(timezone.utc)

    print("Consuming normal-events topic...")
    for message in consumer:
        event = message.value
        batch.append(event)

        now = datetime.now(timezone.utc)
        elapsed = (now - last_flush).total_seconds()

        if len(batch) >= BATCH_SIZE or elapsed >= FLUSH_INTERVAL:
            try:
                write_parquet(s3, batch)
                batch = []
                last_flush = now
            except Exception as ex:
                print(f"[ERROR] Parquet write failed: {ex}")

if __name__ == "__main__":
    main()