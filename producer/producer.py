import json
import time
import random
import math
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# ─── CONFIG ───────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "raw-metrics"
EMIT_INTERVAL = 1.0        # seconds between events
LATE_ARRIVAL_PROB = 0.05   # 5% of events arrive 30s late
SPIKE_PROB = 0.03          # 3% chance of injecting a spike

# Hosts that emit metrics
HOSTS = ["web-01", "web-02", "api-01", "api-02", "worker-01"]

# ─── METRIC DEFINITIONS ───────────────────────────
# Each metric has: base, amplitude (seasonality), noise, spike multiplier
METRICS = {
    "cpu_usage": {
        "base": 40.0,         # base CPU %
        "amplitude": 20.0,    # swings ±20% over the day
        "noise": 5.0,         # random noise
        "spike_mult": 3.0,    # spike = base * spike_mult
        "unit": "percent",
        "min": 0.0,
        "max": 100.0,
    },
    "api_latency": {
        "base": 120.0,        # base latency ms
        "amplitude": 40.0,    # swings ±40ms over the day
        "noise": 15.0,
        "spike_mult": 5.0,
        "unit": "ms",
        "min": 1.0,
        "max": 10000.0,
    },
    "order_volume": {
        "base": 500.0,        # base orders/min
        "amplitude": 300.0,   # swings ±300 over the day
        "noise": 50.0,
        "spike_mult": 4.0,
        "unit": "count",
        "min": 0.0,
        "max": 10000.0,
    },
}

# ─── SEASONALITY ──────────────────────────────────
def seasonal_value(metric_name: str, now: datetime) -> float:
    """
    Simulates realistic daily seasonality using a sine wave.
    Peak at ~14:00 (2pm), trough at ~02:00 (2am).
    """
    cfg = METRICS[metric_name]

    # hour as fraction of day (0.0 → 1.0)
    hour_fraction = (now.hour + now.minute / 60.0) / 24.0

    # sine wave: peak at 14:00 → shift by 9 hours (0.375 of day)
    # sin goes from -1 to 1, we scale by amplitude
    seasonality = cfg["amplitude"] * math.sin(2 * math.pi * (hour_fraction - 0.375))

    # add gaussian noise
    noise = random.gauss(0, cfg["noise"])

    value = cfg["base"] + seasonality + noise

    # clamp to valid range
    value = max(cfg["min"], min(cfg["max"], value))

    return round(value, 4)

# ─── SPIKE INJECTION ──────────────────────────────
def maybe_inject_spike(metric_name: str, value: float) -> tuple[float, bool]:
    if random.random() < SPIKE_PROB:
        spike_value = value * METRICS[metric_name]["spike_mult"]
        spike_value = min(spike_value, METRICS[metric_name]["max"])
        return round(spike_value, 4), True
    return value, False

# ─── LATE ARRIVAL SIMULATION ──────────────────────
def get_event_timestamp(now: datetime) -> tuple[str, bool]:
    if random.random() < LATE_ARRIVAL_PROB:
        # simulate event that actually happened 30s ago
        late_ts = now.timestamp() - 30
        return datetime.fromtimestamp(late_ts, tz=timezone.utc).isoformat(), True
    return now.isoformat(), False

# ─── PRODUCER ─────────────────────────────────────
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",              # wait for broker ack
        retries=3,
        linger_ms=10,            # batch for 10ms before sending
    )

def build_event(metric_name: str, host: str, now: datetime) -> dict:
    value = seasonal_value(metric_name, now)
    value, is_spike = maybe_inject_spike(metric_name, value)
    event_ts, is_late = get_event_timestamp(now)

    return {
        "metric_name": metric_name,
        "host": host,
        "value": value,
        "unit": METRICS[metric_name]["unit"],
        "event_timestamp": event_ts,
        "ingestion_timestamp": now.isoformat(),
        "is_spike": is_spike,           # ground truth label (for testing)
        "is_late_arrival": is_late,     # ground truth label (for testing)
    }

def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    producer = create_producer()
    print(f"Connected. Producing to topic: {TOPIC}")
    print("Press Ctrl+C to stop.\n")

    event_count = 0

    try:
        while True:
            now = datetime.now(timezone.utc)

            # emit one event per metric per host each tick
            for metric_name in METRICS:
                host = random.choice(HOSTS)
                event = build_event(metric_name, host, now)

                # use metric_name:host as partition key
                # so same metric+host always goes to same partition
                key = f"{metric_name}:{host}"

                producer.send(TOPIC, key=key, value=event)
                event_count += 1

                # console log
                spike_flag = "🔴 SPIKE" if event["is_spike"] else ""
                late_flag  = "⏰ LATE"  if event["is_late_arrival"] else ""
                print(
                    f"[{event['event_timestamp']}] "
                    f"{metric_name:<15} | {host:<12} | "
                    f"value={event['value']:>10.4f} {event['unit']:<8} "
                    f"{spike_flag} {late_flag}"
                )

            producer.flush()

            if event_count % 30 == 0:
                print(f"\n--- {event_count} events sent so far ---\n")

            time.sleep(EMIT_INTERVAL)

    except KeyboardInterrupt:
        print(f"\nStopped. Total events sent: {event_count}")
        producer.close()

if __name__ == "__main__":
    main()