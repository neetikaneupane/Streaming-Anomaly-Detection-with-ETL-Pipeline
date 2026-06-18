from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import psycopg2

TIMESCALE_CONN = {
    "host": "timescaledb",
    "port": 5432,
    "dbname": "metrics_db",
    "user": "pipeline_user",
    "password": "pipeline_pass",
}

default_args = {
    "owner": "pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def generate_anomaly_report(**context):
    conn = psycopg2.connect(**TIMESCALE_CONN)
    cur  = conn.cursor()

    # query anomaly stats for last 1 hour
    cur.execute("""
        SELECT
            metric_name,
            COUNT(*)                        AS anomaly_count,
            AVG(deviation_score)            AS avg_deviation,
            MAX(deviation_score)            AS max_deviation,
            host                            AS worst_host
        FROM anomaly_events
        WHERE time >= NOW() - INTERVAL '1 hour'
        GROUP BY metric_name, host
        ORDER BY MAX(deviation_score) DESC
    """)
    rows = cur.fetchall()

    if not rows:
        print("No anomalies in the last hour")
        conn.close()
        return

    # get total event count for anomaly rate
    cur.execute("""
        SELECT metric_name, COUNT(*) as total
        FROM raw_metrics
        WHERE time >= NOW() - INTERVAL '1 hour'
        GROUP BY metric_name
    """)
    totals = {row[0]: row[1] for row in cur.fetchall()}

    report_time = datetime.now(timezone.utc)

    print("\n" + "="*60)
    print(f"ANOMALY REPORT — {report_time.strftime('%Y-%m-%d %H:%M UTC')}")
    print("="*60)

    for row in rows:
        metric_name, anomaly_count, avg_dev, max_dev, worst_host = row
        total = totals.get(metric_name, 1)
        rate  = round(anomaly_count / total * 100, 2)

        print(f"\n📊 {metric_name}")
        print(f"   Anomalies:    {anomaly_count}")
        print(f"   Avg z-score:  {avg_dev:.4f}")
        print(f"   Max z-score:  {max_dev:.4f}")
        print(f"   Anomaly rate: {rate}%")
        print(f"   Worst host:   {worst_host}")

        cur.execute("""
            INSERT INTO anomaly_reports
                (report_time, metric_name, anomaly_count, avg_deviation,
                 max_deviation, anomaly_rate_pct, worst_host, report_window_hrs)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
        """, (report_time, metric_name, anomaly_count, avg_dev, max_dev, rate, worst_host))

    conn.commit()
    conn.close()
    print("\n✅ Report saved to TimescaleDB")


with DAG(
    dag_id="anomaly_report",
    default_args=default_args,
    description="Hourly anomaly summary report → TimescaleDB",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reporting", "pipeline"],
) as dag:

    PythonOperator(task_id="generate_anomaly_report", python_callable=generate_anomaly_report)