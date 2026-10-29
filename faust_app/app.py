import faust
import math
import json
import threading
import time
from datetime import datetime, timezone
from prometheus_client import start_http_server, Counter, Histogram, Gauge

# ─── CONFIG ───────────────────────────────────────
KAFKA_BOOTSTRAP = "kafka:29092"
WINDOW_SIZE_S = 60
WINDOW_GRACE_S = 30
STATE_WINDOW_COUNT = 10
ANOMALY_ZSCORE_THRESHOLD = 3.0
PROMETHEUS_PORT = 9090
DEAD_LETTER_TOPIC = "dead-letter"

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

FAUST_DEAD_LETTERS_TOTAL = Counter(
    "faust_dead_letters_total",
    "Total events routed to the dead-letter topic",
    ["error_type"],
)

# Precision metrics
FAUST_PROCESSING_DURATION_SECONDS = Histogram(
    "faust_processing_duration_seconds",
    "Time spent processing each event (seconds)",
    ["metric_name", "host"],
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5),
)
FAUST_ZSCORE_DISTRIBUTION = Histogram(
    "faust_zscore_distribution",
    "Distribution of z-scores for all evaluated events",
    ["metric_name", "host"],
    buckets=(-10.0, -5.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 5.0, 10.0),
)
FAUST_ANOMALY_BY_DIRECTION = Counter(
    "faust_anomaly_by_direction_total",
    "Anomalies split by deviation direction (positive/negative z-score)",
    ["metric_name", "host", "direction"],
)
FAUST_BASELINE_MEAN = Gauge(
    "faust_baseline_mean",
    "Current weighted baseline mean per metric:host",
    ["metric_name", "host"],
)
FAUST_BASELINE_STDDEV = Gauge(
    "faust_baseline_stddev",
    "Current weighted baseline stddev per metric:host",
    ["metric_name", "host"],
)
FAUST_BASELINE_WINDOWS_USED = Gauge(
    "faust_baseline_windows_used",
    "Number of windows in current baseline per metric:host",
    ["metric_name", "host"],
)
FAUST_STATE_KEYS = Gauge(
    "faust_state_keys",
    "Number of metric:host keys tracked in Faust state table",
)
FAUST_WINDOW_ROLLOVERS = Counter(
    "faust_window_rollovers_total",
    "Times a metric:host window rolled over to a new window",
    ["metric_name", "host"],
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
dead_letter_topic = app.topic(DEAD_LETTER_TOPIC)

# ─── STATE TABLE (plain dicts — fully JSON serializable) ──
metric_states = app.Table("metric-states", default=dict, partitions=3)

# Track unique metric:host keys seen for precise state cardinality
_seen_state_keys = set()

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
        start = time.perf_counter()
        try:
            now = datetime.now(timezone.utc)
            # safe defaults so dead-letter / labels work even on early failure
            metric_name = "unknown"
            host = "unknown"
            metric_key = "unknown:unknown"

            if not isinstance(event, dict):
                event = json.loads(event)

            metric_name = event["metric_name"]
            host        = event["host"]
            value       = float(event["value"])
            metric_key  = f"{metric_name}:{host}"
            late        = is_late(event, now)
            window_start = get_window_start(event["event_timestamp"])
            window_end   = get_window_end(window_start)

            # track unique state keys for precise cardinality
            if metric_key not in _seen_state_keys:
                _seen_state_keys.add(metric_key)
                FAUST_STATE_KEYS.set(len(_seen_state_keys))

            # count every processed event
            FAUST_EVENTS_PROCESSED.labels(topic="raw-metrics", metric_name=metric_name, host=host).inc()
            if late:
                FAUST_LATE_ARRIVALS.labels(metric_name=metric_name, host=host).inc()

            # detect window rollover for precision state tracking
            prior_state = dict(metric_states[metric_key])
            prior_window = prior_state.get("current_window_start")

            # get state, update, persist
            state = dict(metric_states[metric_key])
            state = update_state(state, value, window_start)
            metric_states[metric_key] = state

            # count window rollovers (new window started for this metric:host)
            if prior_window and prior_window != state.get("current_window_start"):
                FAUST_WINDOW_ROLLOVERS.labels(metric_name=metric_name, host=host).inc()

            baseline = get_baseline(state)
            history_len = len(state.get("window_history", []))

            # expose current baseline as gauges for precision querying
            if baseline is not None:
                FAUST_BASELINE_MEAN.labels(metric_name=metric_name, host=host).set(baseline["mean"])
                FAUST_BASELINE_STDDEV.labels(metric_name=metric_name, host=host).set(baseline["stddev"])
                FAUST_BASELINE_WINDOWS_USED.labels(metric_name=metric_name, host=host).set(baseline["windows_used"])

            if baseline is None:
                FAUST_WARMUP_EVENTS.labels(metric_name=metric_name, host=host).inc()
                print(f"[WARMUP] {metric_key:<30} value={value:>10.4f} ({history_len}/{STATE_WINDOW_COUNT} windows)")
                await normal_topic.send(key=metric_key, value=event)
                FAUST_NORMAL_EVENTS.labels(metric_name=metric_name, host=host).inc()
                continue

            zscore    = round((value - baseline["mean"]) / baseline["stddev"], 4)
            threshold = round(baseline["mean"] + ANOMALY_ZSCORE_THRESHOLD * baseline["stddev"], 4)
            is_anomaly = abs(zscore) > ANOMALY_ZSCORE_THRESHOLD

            # record z-score distribution for precision analysis
            FAUST_ZSCORE_DISTRIBUTION.labels(metric_name=metric_name, host=host).observe(zscore)

            if is_anomaly:
                direction = "positive" if zscore > 0 else "negative"
                FAUST_ANOMALIES_DETECTED.labels(metric_name=metric_name, host=host).inc()
                FAUST_ANOMALY_BY_DIRECTION.labels(metric_name=metric_name, host=host, direction=direction).inc()
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
            error_type = type(ex).__name__
            FAUST_ERRORS.labels(error_type=error_type).inc()
            FAUST_DEAD_LETTERS_TOTAL.labels(error_type=error_type).inc()

            # route the failing event to the dead-letter topic for inspection / replay
            dl_record = {
                "error": str(ex),
                "error_type": error_type,
                "failed_at": now.isoformat(),
                "metric_name": metric_name,
                "host": host,
                "original_event": event if isinstance(event, dict) else str(event),
            }
            try:
                await dead_letter_topic.send(key=metric_key, value=dl_record)
                print(f"📨 DEAD-LETTER {metric_key:<30} error={error_type}: {ex}")
            except Exception as send_ex:
                print(f"[ERROR] failed to send dead-letter: {send_ex}")

            print(f"[ERROR] processing event: {ex}")
        finally:
            # record processing duration for every event (success or error)
            duration = time.perf_counter() - start
            try:
                FAUST_PROCESSING_DURATION_SECONDS.labels(metric_name=metric_name, host=host).observe(duration)
            except Exception:
                pass

if __name__ == "__main__":
    app.main()