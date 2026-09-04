// lib.js — shared helpers for the scaling-test harness.
//
// Reuses the project's own Mongoose models (../shared/db) so the harness reads
// the exact same collections the services write. No extra dependencies.

const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const { connectDB, mongoose, Reading, Alert } = require("../shared/db");

const DOCKER = process.env.DOCKER_BIN || "docker";
const SENSORS_DIR = path.join(__dirname, "..", "sensors");
const OUT_DIR = path.join(__dirname, "out");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function ensureOutDir() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  return OUT_DIR;
}

function percentile(sorted, p) {
  if (!sorted.length) return null;
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.max(0, idx)];
}

function stats(arr) {
  if (!arr.length) return { n: 0, p50: null, p95: null, max: null, mean: null };
  const s = [...arr].sort((a, b) => a - b);
  const mean = s.reduce((a, b) => a + b, 0) / s.length;
  return {
    n: s.length,
    p50: percentile(s, 50),
    p95: percentile(s, 95),
    max: s[s.length - 1],
    mean: Math.round(mean),
  };
}

// --- database helpers ------------------------------------------------------
async function resetDb() {
  await Reading.deleteMany({});
  await Alert.deleteMany({});
}

// Server-side aggregation of ingestion latency (createdAt - reading timestamp).
// This is the end-to-end latency the plan asks for (sensor time -> stored),
// which includes MQTT + Node-RED transit, computed in Mongo to avoid pulling
// hundreds of thousands of documents into the harness.
async function ingestionLatency(windowStart, windowEnd) {
  const match = { createdAt: {} };
  if (windowStart) match.createdAt.$gte = windowStart;
  if (windowEnd) match.createdAt.$lte = windowEnd;
  if (!windowStart && !windowEnd) delete match.createdAt;

  const res = await Reading.aggregate([
    { $match: match },
    { $project: { latMs: { $subtract: ["$createdAt", "$timestamp"] } } },
    {
      $group: {
        _id: null,
        count: { $sum: 1 },
        p: { $percentile: { input: "$latMs", p: [0.5, 0.95], method: "approximate" } },
        max: { $max: "$latMs" },
        mean: { $avg: "$latMs" },
      },
    },
  ]);
  if (!res.length) return { count: 0, p50: null, p95: null, max: null, mean: null };
  const r = res[0];
  return {
    count: r.count,
    p50: Math.round(r.p[0]),
    p95: Math.round(r.p[1]),
    max: Math.round(r.max),
    mean: Math.round(r.mean),
  };
}

// --- fleet process ---------------------------------------------------------
// Spawns `node fleet.js` with the given env. onLine is called for every stdout
// line (used to capture "Total published" and "FIRE_IGNITED ...").
function spawnFleet(env, onLine) {
  const child = spawn("node", ["fleet.js"], {
    cwd: SENSORS_DIR,
    env: { ...process.env, ...env },
  });
  let buf = "";
  const handle = (data) => {
    buf += data.toString();
    let nl;
    while ((nl = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      if (onLine) onLine(line);
    }
  };
  child.stdout.on("data", handle);
  child.stderr.on("data", (d) => process.stderr.write(`[fleet:err] ${d}`));
  return child;
}

// SIGINT the fleet and wait for it to exit (it prints "Total published: N").
function stopFleet(child) {
  return new Promise((resolve) => {
    child.once("exit", () => resolve());
    child.kill("SIGINT");
    setTimeout(() => resolve(), 4000); // safety timeout
  });
}

// --- docker stats sampler --------------------------------------------------
// Polls `docker stats --no-stream` on an interval and records CPU%/mem for
// containers whose name contains any of `matchNames`. Returns a controller with
// stop() -> aggregated { container: { cpuAvg, cpuMax, memAvgMB, memMaxMB, n } }.
function startDockerStats(matchNames, intervalMs = 3000) {
  const samples = {}; // name -> { cpu:[], mem:[] }
  let stopped = false;

  function parseMem(s) {
    // e.g. "45.6MiB / 7.5GiB" -> MB used
    const used = s.split("/")[0].trim();
    const m = used.match(/([\d.]+)\s*([KMG]i?B)/i);
    if (!m) return null;
    const v = parseFloat(m[1]);
    const unit = m[2].toLowerCase();
    if (unit.startsWith("k")) return v / 1024;
    if (unit.startsWith("g")) return v * 1024;
    return v; // MiB/MB
  }

  async function tick() {
    if (stopped) return;
    await new Promise((resolve) => {
      const p = spawn(DOCKER, [
        "stats",
        "--no-stream",
        "--format",
        "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}",
      ]);
      let out = "";
      p.stdout.on("data", (d) => (out += d.toString()));
      p.on("error", () => resolve());
      p.on("exit", () => {
        out.split("\n").forEach((line) => {
          const [name, cpu, mem] = line.split("|");
          if (!name) return;
          if (!matchNames.some((m) => name.includes(m))) return;
          samples[name] = samples[name] || { cpu: [], mem: [] };
          const c = parseFloat((cpu || "").replace("%", ""));
          const mb = parseMem(mem || "");
          if (!Number.isNaN(c)) samples[name].cpu.push(c);
          if (mb != null) samples[name].mem.push(mb);
        });
        resolve();
      });
    });
    if (!stopped) setTimeout(tick, intervalMs);
  }
  tick();

  return {
    stop() {
      stopped = true;
      const agg = {};
      for (const [name, s] of Object.entries(samples)) {
        const cpuMax = s.cpu.length ? Math.max(...s.cpu) : null;
        const cpuAvg = s.cpu.length ? s.cpu.reduce((a, b) => a + b, 0) / s.cpu.length : null;
        const memMax = s.mem.length ? Math.max(...s.mem) : null;
        const memAvg = s.mem.length ? s.mem.reduce((a, b) => a + b, 0) / s.mem.length : null;
        agg[name] = {
          n: s.cpu.length,
          cpuAvg: cpuAvg == null ? null : Number(cpuAvg.toFixed(1)),
          cpuMax: cpuMax == null ? null : Number(cpuMax.toFixed(1)),
          memAvgMB: memAvg == null ? null : Number(memAvg.toFixed(1)),
          memMaxMB: memMax == null ? null : Number(memMax.toFixed(1)),
        };
      }
      return agg;
    },
  };
}

// Pick the first aggregated entry whose name contains `needle`.
function pickContainer(agg, needle) {
  const key = Object.keys(agg).find((k) => k.includes(needle));
  return key ? agg[key] : {};
}

// --- detection metrics from container logs ---------------------------------
// Parses `[metrics] detect totalMs=.. queryMs=.. triggered=.. dedup=..` lines
// emitted by the detection service since `sinceISO`.
function detectionMetricsSince(sinceISO) {
  return new Promise((resolve) => {
    const p = spawn(DOCKER, ["compose", "logs", "--no-log-prefix", "--since", sinceISO, "detection"], {
      cwd: path.join(__dirname, ".."),
    });
    let out = "";
    p.stdout.on("data", (d) => (out += d.toString()));
    p.stderr.on("data", () => {});
    p.on("error", () => resolve({ totalMs: [], queryMs: [], triggered: 0, dedup: 0 }));
    p.on("exit", () => {
      const totalMs = [];
      const queryMs = [];
      let triggered = 0;
      let dedup = 0;
      out.split("\n").forEach((line) => {
        if (!line.includes("[metrics] detect")) return;
        const t = line.match(/totalMs=(\d+)/);
        const q = line.match(/queryMs=(\d+)/);
        if (t) totalMs.push(Number(t[1]));
        if (q) queryMs.push(Number(q[1]));
        if (/triggered=true/.test(line)) triggered++;
        const d = line.match(/dedup=(\d)/);
        if (d && d[1] === "1") dedup++;
      });
      resolve({ totalMs, queryMs, triggered, dedup });
    });
  });
}

// --- output writers --------------------------------------------------------
function writeJson(name, obj) {
  ensureOutDir();
  const f = path.join(OUT_DIR, name);
  fs.writeFileSync(f, JSON.stringify(obj, null, 2) + "\n");
  return f;
}

function writeCsv(name, rows) {
  ensureOutDir();
  const f = path.join(OUT_DIR, name);
  if (!rows.length) {
    fs.writeFileSync(f, "");
    return f;
  }
  const headers = Object.keys(rows[0]);
  const lines = [headers.join(",")];
  for (const r of rows) {
    lines.push(headers.map((h) => (r[h] == null ? "" : r[h])).join(","));
  }
  fs.writeFileSync(f, lines.join("\n") + "\n");
  return f;
}

module.exports = {
  connectDB,
  mongoose,
  Reading,
  Alert,
  sleep,
  stats,
  percentile,
  resetDb,
  ingestionLatency,
  spawnFleet,
  stopFleet,
  startDockerStats,
  pickContainer,
  detectionMetricsSince,
  writeJson,
  writeCsv,
  ensureOutDir,
  OUT_DIR,
};
