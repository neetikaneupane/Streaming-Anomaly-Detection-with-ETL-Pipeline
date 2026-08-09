"use strict";

const $ = (sel) => document.querySelector(sel);

const METRICS = ["cpu_usage", "api_latency", "order_volume"];
const METRIC_COLORS = { cpu_usage: "#38bdf8", api_latency: "#a78bfa", order_volume: "#fbbf24" };
const BUF_MAX = 160;
const ANOM_Z_THRESHOLD = 3.0;

const state = {
  connected: false,
  last: null,
  buffers: { cpu_usage: [], api_latency: [], order_volume: [] },
  markers: { cpu_usage: [], api_latency: [], order_volume: [] },
  feed: [],
  anomalyPulse: 0,
};

/* ────────────────────────── FLOW GRAPH ────────────────────────── */

const NODES = {
  producer:     { x: 80,  y: 300, color: "#38bdf8", label: "PRODUCER",      sub: "synthetic metrics" },
  kafka:        { x: 300, y: 300, color: "#f59e0b", label: "KAFKA",         sub: "3 topics" },
  faust:        { x: 520, y: 300, color: "#a78bfa", label: "FAUST",         sub: "z-score detector" },
  normal_sink:  { x: 740, y: 130, color: "#34d399", label: "NORMAL SINK",   sub: "parquet writer" },
  anomaly_sink: { x: 740, y: 470, color: "#fb7185", label: "ANOMALY SINK",  sub: "tsdb writer" },
  minio:        { x: 950, y: 130, color: "#22d3ee", label: "MINIO",         sub: "S3 lake" },
  timescaledb:  { x: 950, y: 470, color: "#2dd4bf", label: "TIMESCALEDB",   sub: "hypertables" },
  airflow:      { x: 740, y: 300, color: "#f472b6", label: "AIRFLOW",       sub: "ETL DAGs" },
};

const EDGES = [
  { from: "producer", to: "kafka" },
  { from: "kafka", to: "faust" },
  { from: "faust", to: "normal_sink" },
  { from: "faust", to: "anomaly_sink" },
  { from: "normal_sink", to: "minio" },
  { from: "anomaly_sink", to: "timescaledb" },
  { from: "minio", to: "airflow" },
  { from: "airflow", to: "timescaledb" },
];

const VW = 1200, VH = 640;
const NN = 118, NH = 48; // node draw size

// particle tracks
const tracks = {};
for (const e of EDGES) {
  const key = e.from + ">" + e.to;
  tracks[key] = [];
  for (let i = 0; i < 3; i++) {
    tracks[key].push({ p: i / 3, speed: 0.0016 + Math.random() * 0.0022, size: 2 + Math.random() * 2 });
  }
}

function nodeCenter(name) {
  const n = NODES[name];
  return { x: n.x, y: n.y };
}

function edgePoints(e) {
  const a = NODES[e.from], b = NODES[e.to];
  const dx = b.x - a.x, dy = b.y - a.y;
  const len = Math.hypot(dx, dy) || 1;
  const ux = dx / len, uy = dy / len;
  const s = { x: a.x + ux * (NN / 2 + 6), y: a.y + uy * (NH / 2 + 6) };
  const t = { x: b.x - ux * (NN / 2 + 6), y: b.y - uy * (NH / 2 + 6) };
  // perpendicular control point for a gentle arc
  const cx = (s.x + t.x) / 2 - uy * 26;
  const cy = (s.y + t.y) / 2 + ux * 26;
  return { s, t, c: { x: cx, y: cy }, color: NODES[e.from].color };
}

function pointOnCurve(s, c, t, f) {
  const a = (1 - f) * (1 - f), b = 2 * (1 - f) * f, cc = f * f;
  return { x: a * s.x + b * c.x + cc * t.x, y: a * s.y + b * c.y + cc * t.y };
}

/* ────────────────────────── ANIMATION LOOP ────────────────────────── */

const flowCanvas = $("#flow");
const flowCtx = flowCanvas.getContext("2d");

function fitFlowCanvas() {
  const wrap = flowCanvas.parentElement;
  const w = wrap.clientWidth;
  const desired = (w / VW) * VH;
  const h = Math.max(240, Math.min(desired, 420));
  const dpr = window.devicePixelRatio || 1;
  flowCanvas.style.height = h + "px";
  flowCanvas.width = Math.floor(w * dpr);
  flowCanvas.height = Math.floor(h * dpr);
}
window.addEventListener("resize", fitFlowCanvas);

let lastFrame = performance.now();
let animating = false;

function active(node, now) {
  return (node._act && now - node._act < 4200) || (node._hot && now - node._hot < 900);
}

function drawFlow(now) {
  const dpr = window.devicePixelRatio || 1;
  const w = flowCanvas.width / dpr, h = flowCanvas.height / dpr;
  const s = Math.min(w / VW, h / VH);
  const ox = (w - VW * s) / 2, oy = (h - VH * s) / 2;
  flowCtx.setTransform(dpr * s, 0, 0, dpr * s, dpr * ox, dpr * oy);
  flowCtx.clearRect(-ox / s, -oy / s, VW + ox * 2 / s, VH + oy * 2 / s);

  // edges
  for (const e of EDGES) {
    const pts = edgePoints(e);
    const a = NODES[e.from], b = NODES[e.to];
    const on = active(a, now) || active(b, now);
    flowCtx.beginPath();
    flowCtx.moveTo(pts.s.x, pts.s.y);
    flowCtx.quadraticCurveTo(pts.c.x, pts.c.y, pts.t.x, pts.t.y);
    flowCtx.strokeStyle = on ? eptsColor(pts.color, 0.85) : "rgba(120,135,160,0.4)";
    flowCtx.lineWidth = on ? 2.6 : 1.8;
    flowCtx.stroke();

    // moving particles
    const key = e.from + ">" + e.to;
    for (const pt of tracks[key]) {
      pt.p += pt.speed;
      if (pt.p >= 1) {
        pt.p = 0;
        b._act = now;          // arrival pulse on target
        if (e.to === "anomaly_sink" || e.to === "timescaledb") a._hot = now;
      }
      const pos = pointOnCurve(pts.s, pts.c, pts.t, pt.p);
      flowCtx.beginPath();
      flowCtx.arc(pos.x, pos.y, pt.size, 0, Math.PI * 2);
      flowCtx.fillStyle = eptsColor(pts.color, 0.9);
      flowCtx.fill();
    }
  }

  // nodes
  for (const name of Object.keys(NODES)) {
    const n = NODES[name];
    const on = active(n, now);
    const x = n.x - NN / 2, y = n.y - NH / 2;

    // body
    flowCtx.beginPath();
    flowCtx.roundRect(x, y, NN, NH, 9);
    flowCtx.fillStyle = on ? "#f1f5fe" : "#ffffff";
    flowCtx.fill();
    flowCtx.strokeStyle = on ? n.color : "#cbd5e1";
    flowCtx.lineWidth = on ? 2.4 : 1.2;
    flowCtx.stroke();

    // left accent bar
    flowCtx.fillStyle = on ? n.color : "#cbd5e1";
    flowCtx.beginPath();
    flowCtx.roundRect(x, y + 6, 4, NH - 12, 2);
    flowCtx.fill();

    // status dot
    flowCtx.beginPath();
    flowCtx.arc(n.x - NN / 2 + 16, n.y - NH / 2 + 13, 3.5, 0, Math.PI * 2);
    flowCtx.fillStyle = on ? n.color : "#cbd5e1";
    flowCtx.fill();

    // label + subtitle
    flowCtx.textAlign = "center";
    flowCtx.fillStyle = "#1c2432";
    flowCtx.font = "600 12px -apple-system, 'Segoe UI', Roboto, sans-serif";
    flowCtx.fillText(n.label, n.x, n.y - 1);
    flowCtx.fillStyle = "#66748f";
    flowCtx.font = "11px -apple-system, 'Segoe UI', Roboto, sans-serif";
    flowCtx.fillText(n.sub, n.x, n.y + 15);
  }
}

function eptsColor(hex, alpha) {
  const h = hex.replace("#", "");
  const r = parseInt(h.slice(0, 2), 16), g = parseInt(h.slice(2, 4), 16), b = parseInt(h.slice(4, 6), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}

function frame(now) {
  const dt = now - lastFrame;
  lastFrame = now;
  if (state.anomalyPulse > 0) state.anomalyPulse -= dt;
  drawFlow(now);
  requestAnimationFrame(frame);
}

/* ────────────────────────── CHARTS ────────────────────────── */

function fitChart(canvas) {
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.floor(canvas.clientWidth * dpr);
  canvas.height = Math.floor(canvas.clientHeight * dpr);
}

function drawChart(canvas, name) {
  const dpr = window.devicePixelRatio || 1;
  const ctx = canvas.getContext("2d");
  const w = canvas.width / dpr, h = canvas.height / dpr;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  const data = state.buffers[name];
  const marks = state.markers[name];
  const pad = 8, top = 14, bottom = 18;
  const plotW = w - pad * 2, plotH = h - top - bottom;

  let lo = Infinity, hi = -Infinity;
  for (const d of data) { if (d.v < lo) lo = d.v; if (d.v > hi) hi = d.v; }
  for (const m of marks) { if (m.v < lo) lo = m.v; if (m.v > hi) hi = m.v; }
  if (!isFinite(lo)) { lo = 0; hi = 1; }
  if (hi - lo < 1e-6) { lo -= 1; hi += 1; }
  const range = hi - lo;

  // grid
  ctx.strokeStyle = "#e2e8f0";
  ctx.lineWidth = 1;
  ctx.beginPath();
  for (let i = 1; i < 4; i++) {
    const y = top + (plotH / 4) * i;
    ctx.moveTo(pad, y); ctx.lineTo(w - pad, y);
  }
  ctx.stroke();

  // baseline at zero
  if (lo < 0 && hi > 0) {
    const y0 = top + (0 - lo) / range * plotH;
    ctx.strokeStyle = "rgba(220,38,38,0.5)";
    ctx.beginPath(); ctx.moveTo(pad, y0); ctx.lineTo(w - pad, y0); ctx.stroke();
  }

  const color = METRIC_COLORS[name];
  const xOf = (i) => pad + (i / (BUF_MAX - 1)) * plotW;
  const yOf = (v) => top + (1 - (v - lo) / range) * plotH;

  // area + line
  if (data.length > 1) {
    ctx.beginPath();
    ctx.moveTo(xOf(0), yOf(data[0].v));
    for (let i = 1; i < data.length; i++) ctx.lineTo(xOf(i), yOf(data[i].v));
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.8;
    ctx.stroke();

    ctx.lineTo(xOf(data.length - 1), top + plotH);
    ctx.lineTo(xOf(0), top + plotH);
    ctx.closePath();
    const g = ctx.createLinearGradient(0, top, 0, top + plotH);
    g.addColorStop(0, eptsColor(color, 0.14));
    g.addColorStop(1, eptsColor(color, 0));
    ctx.fillStyle = g;
    ctx.fill();
  }

  // anomaly markers
  for (const m of marks) {
    ctx.beginPath();
    ctx.arc(xOf(m.i), yOf(m.v), 3.5, 0, Math.PI * 2);
    ctx.fillStyle = "#dc2626";
    ctx.strokeStyle = "#ffffff";
    ctx.lineWidth = 1.5;
    ctx.fill();
    ctx.stroke();
  }

  // labels
  ctx.fillStyle = "#8a96ad";
  ctx.font = "10px -apple-system, 'Segoe UI', Roboto, sans-serif";
  ctx.textAlign = "right";
  ctx.fillText(hi.toFixed(1), w - pad, top - 3);
  ctx.textAlign = "left";
  ctx.fillText(lo.toFixed(1), pad, top - 3);
  ctx.textAlign = "right";
  ctx.fillText((data.length ? data[data.length - 1].v : 0).toFixed(1), w - pad, h - 4);
}

/* ────────────────────────── RENDERERS ────────────────────────── */

function renderStats(s) {
  const r = s.rates, t = Math.max(1, s.interval || 2);
  $("#st-raw").textContent = (r.raw / t).toFixed(1);
  $("#st-anom").textContent = r.anomaly;
  let lag = 0;
  for (const k in s.kafka.lag || {}) if (typeof s.kafka.lag[k] === "number") lag += s.kafka.lag[k];
  $("#st-lag").textContent = lag;
  const db = s.db.raw_metrics;
  $("#st-rows").textContent = typeof db === "number" ? db.toLocaleString() : "—";
  const mo = s.minio && s.minio["normal-events"];
  $("#st-objs").textContent = mo && typeof mo.count === "number"
    ? mo.count.toLocaleString() + (mo.truncated ? "+" : "") : "—";

  const chips = $("#dag-chips");
  chips.innerHTML = "";
  for (const dag of Object.keys(s.airflow || {})) {
    const st = s.airflow[dag];
    const color = st === "success" ? "#34d399" : st === "failed" ? "#fb7185" :
                  st === "running" ? "#38bdf8" : st === "queued" ? "#fbbf24" : "#6b7c9b";
    const div = document.createElement("div");
    div.className = "dag-chip";
    div.innerHTML = `<span>${dag.replace(/_/g, " ").toUpperCase()}</span><span class="st" style="color:${color}">${st}</span>`;
    chips.appendChild(div);
  }
}

function renderDetailTables(s) {
  // kafka
  let kh = "";
  for (const topic of Object.keys(s.kafka.offsets || {}).sort()) {
    const off = s.kafka.offsets[topic];
    kh += `<tr><td>${topic}</td><td>${off}</td></tr>`;
  }
  $("#tbl-kafka").innerHTML = kh || `<tr><td colspan="2" class="muted">no data</td></tr>`;

  // db
  let dh = "";
  const db = s.db || {};
  for (const t of Object.keys(db).sort()) {
    if (typeof db[t] !== "number") continue;
    dh += `<tr><td>${t}</td><td>${db[t].toLocaleString()}</td></tr>`;
  }
  $("#tbl-db").innerHTML = dh || `<tr><td colspan="2" class="muted">no data</td></tr>`;

  // minio
  let mh = "";
  const mo = s.minio || {};
  for (const b of Object.keys(mo).sort()) {
    const v = mo[b];
    if (typeof v.count !== "number") continue;
    mh += `<tr><td>${b}</td><td>${v.count.toLocaleString()}${v.truncated ? "+" : ""}</td></tr>`;
  }
  $("#tbl-minio").innerHTML = mh || `<tr><td colspan="2" class="muted">no data</td></tr>`;

  // faust / airflow
  const r = s.rates;
  $("#faust-info").textContent =
    `window 60s · grace 30s · baseline 10 windows · z>${ANOM_Z_THRESHOLD}  —  ` +
    `${r.normal} normal / ${r.anomaly} anomalies in last interval`;
}

function updateNodeSubs(s) {
  const r = s.rates;
  NODES.producer.sub = `raw ${r.raw} ev/2s`;
  NODES.kafka.sub = "3 topics · live";
  NODES.faust.sub = `${r.normal} normal · ${r.anomaly} anom`;
  NODES.normal_sink.sub = `${r.normal} ev/2s`;
  NODES.anomaly_sink.sub = `${r.anomaly} ev/2s`;
  const db = s.db || {};
  NODES.minio.sub = (s.minio && s.minio["normal-events"]) ? `${s.minio["normal-events"].count} objects` : "S3 lake";
  NODES.timescaledb.sub = typeof db.anomaly_events === "number" ? `${db.anomaly_events} anomalies` : "hypertables";
  const a = s.airflow || {};
  NODES.airflow.sub = Object.values(a).filter((v) => v === "running" || v === "queued").length
    ? "running…" : "idle";

  const now = performance.now();
  NODES.producer._act = r.raw > 0 ? now : NODES.producer._act;
  NODES.kafka._act = r.total > 0 ? now : NODES.kafka._act;
  NODES.faust._act = r.normal + r.anomaly > 0 ? now : NODES.faust._act;
  NODES.normal_sink._act = r.normal > 0 ? now : NODES.normal_sink._act;
  NODES.anomaly_sink._act = r.anomaly > 0 ? now : NODES.anomaly_sink._act;
  NODES.minio._act = r.normal > 0 ? now : NODES.minio._act;
  NODES.timescaledb._act = r.anomaly > 0 ? now : NODES.timescaledb._act;
  const busy = Object.values(a).some((v) => v === "running" || v === "queued");
  NODES.airflow._act = busy ? now : NODES.airflow._act;
}

function renderFeed(anomalies) {
  for (const a of anomalies) {
    const exists = state.feed.some((f) => f.ts === a.time && f.metric === a.metric && f.host === a.host);
    if (exists) continue;
    state.feed.unshift(a);
  }
  state.feed = state.feed.slice(0, 12);
  const ul = $("#feed");
  if (!state.feed.length) {
    ul.innerHTML = `<li class="feed-empty">waiting for anomalies…</li>`;
    return;
  }
  ul.innerHTML = state.feed.map((f) => `
    <li>
      <div class="f-head">${f.metric} · ${f.host}</div>
      <div class="f-meta">${fmtTime(f.time)} · value ${f.value}</div>
      <div class="f-meta">z-score <span class="f-z">${f.z}</span></div>
    </li>`).join("");
}

function fmtTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (isNaN(d)) return iso;
  return d.toTimeString().slice(0, 8);
}

/* ────────────────────────── SSE ────────────────────────── */

function connect() {
  const es = new EventSource("/api/events");

  es.onopen = () => {
    state.connected = true;
    $("#conn").textContent = "live";
    $("#conn").className = "conn up";
  };

  es.onerror = () => {
    state.connected = false;
    $("#conn").textContent = "reconnecting";
    $("#conn").className = "conn down";
  };

  es.onmessage = (ev) => {
    let msg;
    try { msg = JSON.parse(ev.data); } catch { return; }

    if (msg.type === "snapshot") {
      state.last = msg;
      msg.interval = 2;
      renderStats(msg);
      renderDetailTables(msg);
      updateNodeSubs(msg);
      if (msg.anomalies_recent && msg.anomalies_recent.length) renderFeed(msg.anomalies_recent);
    } else if (msg.type === "event") {
      if (msg.kind === "raw" && METRICS.includes(msg.metric)) {
        pushValue(msg.metric, msg.value);
      } else if (msg.kind === "anomaly" && METRICS.includes(msg.metric)) {
        pushValue(msg.metric, msg.value);
        const i = state.buffers[msg.metric].length - 1;
        state.markers[msg.metric].push({ i, v: msg.value });
        if (state.markers[msg.metric].length > 40) state.markers[msg.metric].shift();
        state.feed.unshift({ time: msg.ts, metric: msg.metric, host: msg.host, value: msg.value, z: msg.z });
        state.feed = state.feed.slice(0, 12);
        renderFeed([]);
        state.anomalyPulse = 1200;
        NODES.anomaly_sink._act = performance.now();
        NODES.timescaledb._act = performance.now();
      }
    }
  };
}

function pushValue(metric, value) {
  const buf = state.buffers[metric];
  buf.push({ i: buf.length, v: Number(value) });
  if (buf.length > BUF_MAX) buf.shift();
}

/* ────────────────────────── INIT ────────────────────────── */

const CHART_IDS = { cpu_usage: "#chart-cpu", api_latency: "#chart-lat", order_volume: "#chart-vol" };

function initCharts() {
  for (const m of METRICS) fitChart($(CHART_IDS[m]));
}

function drawAllCharts() {
  for (const m of METRICS) drawChart($(CHART_IDS[m]), m);
}

function boot() {
  fitFlowCanvas();
  initCharts();
  window.addEventListener("resize", () => {
    fitFlowCanvas();
    initCharts();
  });
  connect();
  requestAnimationFrame(frame);
  setInterval(drawAllCharts, 200);
  setInterval(() => {
    const now = new Date();
    $("#clock").textContent = now.toTimeString().slice(0, 8);
  }, 1000);
}

document.addEventListener("DOMContentLoaded", boot);
