import faust
import math
import json
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

# ─── CONFIG ───────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
WINDOW_SIZE_S = 60          # 1-minute tumbling windows
WINDOW_GRACE_S = 30         # accept late events up to 30s late
STATE_WINDOW_COUNT = 10     # use last N windows to compute baseline
ANOMALY_ZSCORE_THRESHOLD = 3.0   # flag if z-score > 3

# ─── FAUST APP ────────────────────────────────────
app = faust.App(
    "anomaly-detector",
    broker=f"kafka://{KAFKA_BOOTSTRAP}",
    store="memory://",          # RocksDB state store
    topic_partitions=3,
    consumer_auto_offset_reset="earliest",
)

# ─── TOPICS ───────────────────────────────────────
raw_metrics_topic = app.topic("raw-metrics")
anomalies_topic   = app.topic("anomalies",   value_type=bytes)
normal_topic      = app.topic("normal-events", value_type=bytes)

# ─── STATE MODELS ─────────────────────────────────
@dataclass
class WindowStats:
    """Stats for a single completed window."""
    mean: float = 0.0
    stddev: float = 0.0
    count: int = 0
    window_start: str = ""
    window_end: str = ""

@dataclass
class MetricState:
    """
    Rolling state per metric_key (metric_name:host).
    Keeps last N window stats to compute adaptive baseline.
    """
    # circular buffer of last N window stats
    window_history: list = field(default_factory=list)

    # running accumulators for CURRENT window
    current_sum: float = 0.0
    current_sum_sq: float = 0.0
    current_count: int = 0
    current_window_start: str = ""

    def update(self, value: float, window_start: str):
        """Add a value to the current window accumulator."""
        if self.current_window_start != window_start:
            # new window started — finalise the old one
            self._finalise_window()
            self.current_window_start = window_start
            self.current_sum = 0.0
            self.current_sum_sq = 0.0
            self.current_count = 0

        self.current_sum += value
        self.current_sum_sq += value * value
        self.current_count += 1

    def _finalise_window(self):
        """Compute mean/stddev for completed window, push to history."""
        if self.current_count == 0:
            return
        mean = self.current_sum / self.current_count
        variance = max(
            0.0,
            (self.current_sum_sq / self.current_count) - (mean * mean)
        )
        stddev = math.sqrt(variance)

        stats = {
            "mean": round(mean, 4),
            "stddev": round(stddev, 4),
            "count": self.current_count,
            "window_start": self.current_window_start,
        }
        self.window_history.append(stats)

        # keep only last N windows
        if len(self.window_history) > STATE_WINDOW_COUNT:
            self.window_history = self.window_history[-STATE_WINDOW_COUNT:]

    def get_baseline(self) -> Optional[dict]:
        """
        Compute adaptive baseline from last N completed windows.
        Returns None if not enough history yet (need at least 2 windows).
        """
        if len(self.window_history) < 2:
            return None

        # weighted average — more recent windows count more
        total_weight = 0.0
        weighted_mean = 0.0
        weighted_var  = 0.0

        n = len(self.window_history)
        for i, w in enumerate(self.window_history):
            weight = i + 1   # older = lower weight, newer = higher weight
            weighted_mean += weight * w["mean"]
            weighted_var  += weight * (w["stddev"] ** 2)
            total_weight  += weight

        baseline_mean   = weighted_mean / total_weight
        baseline_stddev = math.sqrt(weighted_var / total_weight)

        # ensure stddev is never 0 (avoid div by zero)
        baseline_stddev = max(baseline_stddev, 0.001)

        return {
            "mean":   round(baseline_mean, 4),
            "stddev": round(baseline_stddev, 4),
            "windows_used": n,
        }

# ─── STATE TABLE ──────────────────────────────────
# Persisted in RocksDB — survives restarts
metric_states = app.Table(
    "metric-states",
    default=MetricState,
    partitions=3,
)

# ─── HELPERS ──────────────────────────────────────
def get_window_start(event_ts: str) -> str:
    """Snap an ISO timestamp to its 1-minute tumbling window bucket."""
    dt = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
    bucket = dt.replace(second=0, microsecond=0)
    return bucket.isoformat()

def get_window_end(window_start: str) -> str:
    from datetime import timedelta
    dt = datetime.fromisoformat(window_start)
    return (dt + timedelta(seconds=WINDOW_SIZE_S)).isoformat()

def compute_zscore(value: float, mean: float, stddev: float) -> float:
    return round((value - mean) / stddev, 4)

def is_late_arrival(event: dict, now: datetime) -> bool:
    event_dt = datetime.fromisoformat(
        event["event_timestamp"].replace("Z", "+00:00")
    )
    delay = (now - event_dt).total_seconds()
    return delay > WINDOW_GRACE_S

# ─── MAIN AGENT ───────────────────────────────────
@app.agent(raw_metrics_topic)
async def process_metrics(stream):
    async for event_bytes in stream:
        now = datetime.now(timezone.utc)

        # deserialise
        event = event_bytes if isinstance(event_bytes, dict) else json.loads(event_bytes)

        metric_name = event["metric_name"]
        host        = event["host"]
        value       = float(event["value"])
        metric_key  = f"{metric_name}:{host}"

        # watermark check — handle late arrivals
        late = is_late_arrival(event, now)

        # get or init state for this metric key
        state: MetricState = metric_states[metric_key]

        # snap event to its window bucket
        window_start = get_window_start(event["event_timestamp"])
        window_end   = get_window_end(window_start)

        # update rolling state
        state.update(value, window_start)
        metric_states[metric_key] = state   # persist back to RocksDB

        # get adaptive baseline from history
        baseline = state.get_baseline()

        if baseline is None:
            # not enough history yet — just log and sink as normal
            print(
                f"[WARMUP] {metric_key:<30} value={value:>10.4f} "
                f"(building baseline, {len(state.window_history)} windows so far)"
            )
            await normal_topic.send(
                key=metric_key,
                value=json.dumps(event).encode()
            )
            continue

        # compute z-score against adaptive baseline
        zscore    = compute_zscore(value, baseline["mean"], baseline["stddev"])
        threshold = baseline["mean"] + ANOMALY_ZSCORE_THRESHOLD * baseline["stddev"]
        is_anomaly = abs(zscore) > ANOMALY_ZSCORE_THRESHOLD

        if is_anomaly:
            enriched = {
                **event,
                "baseline_mean":    baseline["mean"],
                "baseline_stddev":  baseline["stddev"],
                "deviation_score":  zscore,
                "threshold":        round(threshold, 4),
                "window_start":     window_start,
                "window_end":       window_end,
                "windows_used":     baseline["windows_used"],
                "is_late_arrival":  late,
                "detected_at":      now.isoformat(),
            }
            await anomalies_topic.send(
                key=metric_key,
                value=json.dumps(enriched).encode()
            )
            print(
                f" ANOMALY  {metric_key:<30} "
                f"value={value:>10.4f}  "
                f"baseline={baseline['mean']:>10.4f} ± {baseline['stddev']:.4f}  "
                f"z={zscore:>7.4f}  "
                f"{' LATE' if late else ''}"
            )
        else:
            await normal_topic.send(
                key=metric_key,
                value=json.dumps(event).encode()
            )
            print(
                f" normal   {metric_key:<30} "
                f"value={value:>10.4f}  "
                f"baseline={baseline['mean']:>10.4f} ± {baseline['stddev']:.4f}  "
                f"z={zscore:>7.4f}"
            )

if __name__ == "__main__":
    app.main()