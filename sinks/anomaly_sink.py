import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TIMESCALE_CONN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "metrics_db",
    "user": "pipeline_user",
    "password": "pipeline_pass",
}

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
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    print("Consuming anomalies topic...")
    batch = []
    BATCH_SIZE = 10

    for message in consumer:
        event = message.value
        batch.append(event)
        print(f"🚨 [{event['metric_name']}:{event['host']}] "
              f"value={event['value']:.4f} z={event['deviation_score']:.4f}")

        if len(batch) >= BATCH_SIZE:
            try:
                cur = conn.cursor()
                for e in batch:
                    insert_anomaly(cur, e)
                conn.commit()
                print(f"✅ Committed {len(batch)} anomalies to TimescaleDB")
                batch = []
            except Exception as ex:
                conn.rollback()
                print(f"[ERROR] DB insert failed: {ex}")

if __name__ == "__main__":
    main()