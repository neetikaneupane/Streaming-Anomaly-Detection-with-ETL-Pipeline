from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import pyarrow.parquet as pq
import psycopg2
import io
import json

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS   = "minio_admin"
MINIO_SECRET   = "minio_password"
BUCKET         = "normal-events"

TIMESCALE_CONN = {
    "host": "timescaledb",
    "port": 5432,
    "dbname": "metrics_db",
    "user": "pipeline_user",
    "password": "pipeline_pass",
}

EXPECTED_COLUMNS = {"metric_name", "host", "value", "event_timestamp", "unit"}
VALUE_RANGES = {
    "cpu_usage":    (0, 100),
    "api_latency":  (0, 10000),
    "order_volume": (0, 10000),
}

default_args = {
    "owner": "pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def run_quality_checks(**context):
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )

    conn = psycopg2.connect(**TIMESCALE_CONN)
    cur  = conn.cursor()

    response = s3.list_objects_v2(Bucket=BUCKET)
    if "Contents" not in response:
        print("No files to check")
        return

    files = [o["Key"] for o in response["Contents"] if o["Key"].endswith(".parquet")]
    print(f"Running quality checks on {len(files)} files")

    for key in files:
        issues = []
        passed = True

        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            buf = io.BytesIO(obj["Body"].read())
            df  = pq.read_table(buf).to_pandas()

            row_count  = len(df)
            null_count = int(df.isnull().sum().sum())

            # check 1: schema
            schema_valid = EXPECTED_COLUMNS.issubset(set(df.columns))
            if not schema_valid:
                missing = EXPECTED_COLUMNS - set(df.columns)
                issues.append(f"Missing columns: {missing}")
                passed = False

            # check 2: nulls > 5%
            if null_count / max(row_count * len(df.columns), 1) > 0.05:
                issues.append(f"High null rate: {null_count} nulls in {row_count} rows")
                passed = False

            # check 3: value ranges
            value_range_ok = True
            if "value" in df.columns and "metric_name" in df.columns:
                for metric, (lo, hi) in VALUE_RANGES.items():
                    subset = df[df["metric_name"] == metric]["value"]
                    if len(subset) > 0:
                        out_of_range = subset[(subset < lo) | (subset > hi)]
                        if len(out_of_range) > 0:
                            issues.append(f"{metric}: {len(out_of_range)} values out of range [{lo},{hi}]")
                            value_range_ok = False
                            passed = False

            # check 4: row count sanity
            if row_count == 0:
                issues.append("Empty file")
                passed = False

        except Exception as e:
            issues.append(f"Read error: {str(e)}")
            passed = False
            row_count  = 0
            null_count = 0
            schema_valid = False
            value_range_ok = False

        cur.execute("""
            INSERT INTO data_quality_log
                (file_path, row_count, null_count, schema_valid, value_range_ok, issues, passed)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            key, row_count, null_count,
            schema_valid, value_range_ok,
            "; ".join(issues) if issues else None,
            passed,
        ))

        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} | {key} | rows={row_count} nulls={null_count}")

    conn.commit()
    conn.close()


with DAG(
    dag_id="data_quality_check",
    default_args=default_args,
    description="Data quality checks on MinIO Parquet files",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["quality", "pipeline"],
) as dag:

    PythonOperator(task_id="run_quality_checks", python_callable=run_quality_checks)