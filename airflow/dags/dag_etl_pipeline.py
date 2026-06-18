from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
import psycopg2

# ─── CONFIG ───────────────────────────────────────
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS   = "minio_admin"
MINIO_SECRET   = "minio_password"
BUCKET         = "normal-events"
OUTPUT_BUCKET  = "etl-processed"

TIMESCALE_CONN = {
    "host": "timescaledb",
    "port": 5432,
    "dbname": "metrics_db",
    "user": "pipeline_user",
    "password": "pipeline_pass",
}

default_args = {
    "owner": "pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# ─── TASKS ────────────────────────────────────────
def extract_from_minio(**context):
    """List and read all parquet files from MinIO normal-events bucket."""
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )

    response = s3.list_objects_v2(Bucket=BUCKET)
    if "Contents" not in response:
        print("No files found in bucket")
        return []

    files = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".parquet")]
    print(f"Found {len(files)} parquet files")

    dfs = []
    for key in files:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        df = pq.read_table(buf).to_pandas()
        dfs.append(df)

    if not dfs:
        return []

    combined = pd.concat(dfs, ignore_index=True)
    print(f"Extracted {len(combined)} total records")

    # convert timestamp column to string BEFORE JSON serialization
    if "event_timestamp" in combined.columns:
     combined["event_timestamp"] = combined["event_timestamp"].astype(str)

    context["ti"].xcom_push(key="raw_records", value=combined.to_json())
    return len(combined)


def transform(**context):
    """Clean, validate, and aggregate the extracted records."""
    raw_json = context["ti"].xcom_pull(key="raw_records", task_ids="extract")
    if not raw_json:
        print("No data to transform")
        return

    df = pd.read_json(raw_json)

    # clean
    df = df.dropna(subset=["metric_name", "host", "value"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df[df["value"] > 0]
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["event_timestamp"])

    # floor to 15-minute windows
    df["window"] = df["event_timestamp"].dt.floor("15min")

    # aggregate per metric + host + window
    summary = df.groupby(["window", "metric_name", "host"]).agg(
        avg_value   = ("value", "mean"),
        min_value   = ("value", "min"),
        max_value   = ("value", "max"),
        stddev_value= ("value", "std"),
        event_count = ("value", "count"),
    ).reset_index()

    summary["anomaly_count"] = 0  # normal events — no anomalies
    summary["processed_at"]  = datetime.now(timezone.utc).isoformat()

    # convert window timestamp to ISO string BEFORE JSON serialization
    summary["window"] = summary["window"].astype(str)

    print(f"Transformed into {len(summary)} summary rows")
    context["ti"].xcom_push(key="summary", value=summary.to_json())


def load_to_timescale(**context):
    """Load aggregated summaries into TimescaleDB."""
    summary_json = context["ti"].xcom_pull(key="summary", task_ids="transform")
    if not summary_json:
        print("No summary to load")
        return

    summary = pd.read_json(summary_json)
    summary["window"] = pd.to_datetime(summary["window"], utc=True)
    conn = psycopg2.connect(**TIMESCALE_CONN)
    cur  = conn.cursor()

    inserted = 0
    for _, row in summary.iterrows():
        cur.execute("""
            INSERT INTO etl_metric_summaries
                (time, metric_name, host, avg_value, min_value, max_value,
                 stddev_value, event_count, anomaly_count, processed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            row["window"],
            row["metric_name"],
            row["host"],
            row["avg_value"],
            row["min_value"],
            row["max_value"],
            row.get("stddev_value", 0),
            int(row["event_count"]),
            int(row["anomaly_count"]),
            row["processed_at"],
        ))
        inserted += 1

    conn.commit()
    conn.close()
    print(f"✅ Loaded {inserted} summary rows into TimescaleDB")


def archive_processed(**context):
    """Move processed files to etl-processed bucket."""
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )

    response = s3.list_objects_v2(Bucket=BUCKET)
    if "Contents" not in response:
        return

    moved = 0
    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue
        archive_key = f"archived/{key}"
        s3.copy_object(
            Bucket=OUTPUT_BUCKET,
            CopySource={"Bucket": BUCKET, "Key": key},
            Key=archive_key,
        )
        s3.delete_object(Bucket=BUCKET, Key=key)
        moved += 1

    print(f"✅ Archived {moved} files to {OUTPUT_BUCKET}/archived/")


# ─── DAG ──────────────────────────────────────────
with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="ETL: MinIO Parquet → transform → TimescaleDB",
    schedule_interval="*/15 * * * *",   # every 15 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "pipeline"],
) as dag:

    t_extract   = PythonOperator(task_id="extract",   python_callable=extract_from_minio)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_load      = PythonOperator(task_id="load",      python_callable=load_to_timescale)
    t_archive   = PythonOperator(task_id="archive",   python_callable=archive_processed)

    t_extract >> t_transform >> t_load >> t_archive