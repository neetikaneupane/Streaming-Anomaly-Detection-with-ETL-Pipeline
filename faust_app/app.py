import faust
import math
import json
import threading
from datetime import datetime, timezone
from prometheus_client import start_http_server, Counter

# ─── CONFIG ───────────────────────────────────────
KAFKA_BOOTSTRAP = "kafka:29092"
WINDOW_SIZE_S = 60
WINDOW_GRACE_S = 30
STATE_WINDOW_COUNT = 10
ANOMALY_ZSCORE_THRESHOLD = 3.0
PROMETHEUS_PORT = 9090

# ─── PROMETHEUS METRICS ───────────────────────────────────────
FAUST_EVENTS_PROCESSED = Counter(
    "faust_events_processed_total",
    "Total events processed by the Faust worker",
    ["topic", "metric_name", "host"],
)
FAUST_ANOMALIES_DETECTED = Counter(
    "faust_anomalies_detected_total",
    "Total anomalies detected",
    ["metric_name", "host"],
)
FAUST_NORMAL_EVENTS = Counter(
    "faust_normal_events_total",
    "Total normal events routed to normal-events topic",
    ["metric_name", "host"],
)
FAUST_WARMUP_EVENTS = Counter(
    "faust_warmup_events_total",
    "Total events during warmup (insufficient baseline history)",
    ["metric_name", "host"],
)
FAUST_LATE_ARRIVALS = Counter(
    "faust_late_arrivals_total",
    "Total late-arriving events",
    ["metric_name", "host"],
)
FAUST_ERRORS = Counter(
    "faust_processing_errors_total",
    "Total processing errors",
    ["error_type"],
)

# Start Prometheus metrics HTTP server on port 9090 (daemon thread)
threading.Thread(target=start_http_server, args=(PROMETHEUS_PORT,), daemon=True).start()

app = faust.App(
    "anomaly-detector",
    broker=f"kafka://{KAFKA_BOOTSTRAP}",
    store="rocksdb://",
    topic_partitions=3,
    consumer_auto_offset_reset="earliest",
)

# ─── TOPICS ───────────────────────────────────────
raw_metrics_topic = app.topic("raw-metrics")
anomalies_topic   = app.topic("anomalies")
normal_topic      = app.topic("normal-events")

# ─── STATE TABLE (plain dicts — fully JSON serializable) ──
metric_states = app.Table("metric-states", default=dict, partitions=3)

# ─── HELPERS ──────────────────────────────────────
def get_window_start(event_ts: str) -> str:
    dt = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
    return dt.replace(second=0, microsecond=0).isoformat()

def get_window_end(window_start: str) -> str:
    from datetime import timedelta
    return (datetime.fromisoformat(window_start) + timedelta(seconds=WINDOW_SIZE_S)).isoformat()

def is_late(event: dict, now: datetime) -> bool:
    event_dt = datetime.fromisoformat(event["event_timestamp"].replace("Z", "+00:00"))
    return (now - event_dt).total_seconds() > WINDOW_GRACE_S

def update_state(state: dict, value: float, window_start: str) -> dict:
    if state.get("current_window_start") != window_start:
        # finalise old window
        if state.get("current_count", 0) > 0:
            mean = state["current_sum"] / state["current_count"]
            variance = max(0.0, (state["current_sum_sq"] / state["current_count"]) - mean ** 2)
            history = state.get("window_history", [])
            history.append({
                "mean": round(mean, 4),
                "stddev": round(math.sqrt(variance), 4),
                "count": state["current_count"],
                "window_start": state["current_window_start"],
            })
            state["window_history"] = history[-STATE_WINDOW_COUNT:]

        state["current_window_start"] = window_start
        state["current_sum"] = 0.0
        state["current_sum_sq"] = 0.0
        state["current_count"] = 0

    state["current_sum"]    = state.get("current_sum", 0.0) + value
    state["current_sum_sq"] = state.get("current_sum_sq", 0.0) + value * value
    state["current_count"]  = state.get("current_count", 0) + 1
    return state

def get_baseline(state: dict) -> dict | None:
    history = state.get("window_history", [])
    if len(history) < 2:
        return None

    total_weight, weighted_mean, weighted_var = 0.0, 0.0, 0.0
    for i, w in enumerate(history):
        weight = i + 1
        weighted_mean += weight * w["mean"]
        weighted_var  += weight * (w["stddev"] ** 2)
        total_weight  += weight

    mean   = weighted_mean / total_weight
    stddev = max(math.sqrt(weighted_var / total_weight), 0.001)
    return {"mean": round(mean, 4), "stddev": round(stddev, 4), "windows_used": len(history)}

# ─── MAIN AGENT ───────────────────────────────────
@app.agent(raw_metrics_topic)
async def process_metrics(stream):
    async for event in stream:
        try:
            now = datetime.now(timezone.utc)

            if not isinstance(event, dict):
                event = json.loads(event)

            metric_name = event["metric_name"]
            host        = event["host"]
            value       = float(event["value"])
            metric_key  = f"{metric_name}:{host}"
            late        = is_late(event, now)
            window_start = get_window_start(event["event_timestamp"])
            window_end   = get_window_end(window_start)

            # count every processed event
            FAUST_EVENTS_PROCESSED.labels(topic="raw-metrics", metric_name=metric_name, host=host).inc()
            if late:
                FAUST_LATE_ARRIVALS.labels(metric_name=metric_name, host=host).inc()

            # get state, update, persist
            state = dict(metric_states[metric_key])
            state = update_state(state, value, window_start)
            metric_states[metric_key] = state

            baseline = get_baseline(state)
            history_len = len(state.get("window_history", []))

            if baseline is None:
                FAUST_WARMUP_EVENTS.labels(metric_name=metric_name, host=host).inc()
                print(f"[WARMUP] {metric_key:<30} value={value:>10.4f} ({history_len}/{STATE_WINDOW_COUNT} windows)")
                await normal_topic.send(key=metric_key, value=event)
                FAUST_NORMAL_EVENTS.labels(metric_name=metric_name, host=host).inc()
                continue

            zscore    = round((value - baseline["mean"]) / baseline["stddev"], 4)
            threshold = round(baseline["mean"] + ANOMALY_ZSCORE_THRESHOLD * baseline["stddev"], 4)
            is_anomaly = abs(zscore) > ANOMALY_ZSCORE_THRESHOLD

            if is_anomaly:
                FAUST_ANOMALIES_DETECTED.labels(metric_name=metric_name, host=host).inc()
                enriched = {
                    **event,
                    "baseline_mean":   baseline["mean"],
                    "baseline_stddev": baseline["stddev"],
                    "deviation_score": zscore,
                    "threshold":       threshold,
                    "window_start":    window_start,
                    "window_end":      window_end,
                    "windows_used":    baseline["windows_used"],
                    "is_late_arrival": late,
                    "detected_at":     now.isoformat(),
                }
                await anomalies_topic.send(key=metric_key, value=enriched)
                print(f"🚨 ANOMALY  {metric_key:<30} value={value:>10.4f}  baseline={baseline['mean']:>10.4f} ± {baseline['stddev']:.4f}  z={zscore:>7.4f} {'⏰ LATE' if late else ''}")
            else:
                FAUST_NORMAL_EVENTS.labels(metric_name=metric_name, host=host).inc()
                await normal_topic.send(key=metric_key, value=event)
                print(f"✅ normal   {metric_key:<30} value={value:>10.4f}  baseline={baseline['mean']:>10.4f} ± {baseline['stddev']:.4f}  z={zscore:>7.4f}")
        except Exception as ex:
            FAUST_ERRORS.labels(error_type=type(ex).__name__).inc()
            print(f"[ERROR] processing event: {ex}")

if __name__ == "__main__":
    app.main()