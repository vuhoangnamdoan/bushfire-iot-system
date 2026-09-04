// plan2_burst.js — Event-intensity burst test (simultaneous fires).
//
// Runs a calm baseline, then a burst where nodes across all regions ignite at
// once (FORCE_FIRE). Measures how detection + alert cope with a surge of
// over-threshold readings, and confirms the alert cooldown prevents a storm.
//
// Records:
//   - detection response time (totalMs) and recent-window read latency (queryMs),
//     p50/p95, from the detection service's [metrics] logs
//   - alert lag: alert.timestamp - ignition time (per alert)
//   - detect calls vs alerts written (cooldown effect)
//   - CPU/mem of detection, alert, mongodb, ingestion
//
// Usage (from benchmarks/):
//   node plan2_burst.js
//   NODES=100 BASELINE_SEC=30 SPIKE_SEC=90 INTERVAL_MS=5000 node plan2_burst.js
//
// Keep NODES*(1000/INTERVAL_MS) similar to a Plan 1 step so you isolate
// intensity, not raw volume.

const lib = require("./lib");

const NODES = Number(process.env.NODES || 100);
const REGIONS = Number(process.env.REGIONS || 5);
const INTERVAL_MS = Number(process.env.INTERVAL_MS || 5000);
const BASELINE_SEC = Number(process.env.BASELINE_SEC || 30);
const SPIKE_SEC = Number(process.env.SPIKE_SEC || 90);
const BURST_AT_SEC = Number(process.env.BURST_AT_SEC || 3);
const DRAIN_SEC = Number(process.env.DRAIN_SEC || 3);

async function phase(name, env, seconds, onLine) {
  console.log(`\n=== PHASE: ${name} (${seconds}s, ${NODES} nodes, ~${(NODES / (INTERVAL_MS / 1000)).toFixed(1)} msg/s) ===`);
  const child = lib.spawnFleet(
    { NODES: String(NODES), REGIONS: String(REGIONS), PUBLISH_INTERVAL_MS: String(INTERVAL_MS), PUBLISH_STAGGER_MS: String(INTERVAL_MS), TIME_SCALE: "1", ...env },
    onLine
  );
  await lib.sleep(seconds * 1000);
  await lib.stopFleet(child);
}

async function main() {
  await lib.connectDB();
  await lib.resetDb();

  const sampler = lib.startDockerStats(["ingestion", "detection", "alert", "mongodb"]);

  // 1) Baseline — no fires, detection should be idle.
  await phase("baseline", { IGNITION_CHANCE: "0", FAULT_RATE: "0" }, BASELINE_SEC);

  // 2) Spike — force all nodes to ignite at once.
  const spikeStart = new Date();
  let igniteTime = null;
  await phase(
    "spike",
    { IGNITION_CHANCE: "0", FAULT_RATE: "0", FORCE_FIRE: "1", BURST_AT_SEC: String(BURST_AT_SEC), BURST_FRACTION: "1" },
    SPIKE_SEC,
    (line) => {
      const m = line.match(/FIRE_IGNITED\s+(\d+).*at\s+(\S+)/);
      if (m) {
        igniteTime = new Date(m[2]);
        console.log(`  [harness] ignition detected at ${m[2]} (${m[1]} nodes)`);
      }
    }
  );

  await lib.sleep(DRAIN_SEC * 1000);
  const spikeEnd = new Date();
  const agg = sampler.stop();

  // Detection metrics from logs (since spike start).
  const det = await lib.detectionMetricsSince(spikeStart.toISOString());
  const totalStats = lib.stats(det.totalMs);
  const queryStats = lib.stats(det.queryMs);

  // Alerts written during the spike, with lag from ignition.
  const alerts = await lib.Alert.find({ timestamp: { $gte: spikeStart, $lte: spikeEnd } }).lean();
  const lags = igniteTime ? alerts.map((a) => new Date(a.timestamp) - igniteTime) : [];
  const lagStats = lib.stats(lags);
  const regionsAlerted = [...new Set(alerts.map((a) => a.region))];

  const summary = {
    config: { NODES, REGIONS, INTERVAL_MS, BASELINE_SEC, SPIKE_SEC },
    igniteTime: igniteTime ? igniteTime.toISOString() : null,
    detectCalls: det.totalMs.length,
    detectTriggered: det.triggered,
    detectDedup: det.dedup,
    alertsWritten: alerts.length,
    regionsAlerted: regionsAlerted.length,
    cooldownSuppressed: det.triggered - alerts.length, // triggered detects that did NOT create a new alert
    detectTotalMs: { p50: totalStats.p50, p95: totalStats.p95, max: totalStats.max, mean: totalStats.mean, n: totalStats.n },
    mongoReadMs: { p50: queryStats.p50, p95: queryStats.p95, max: queryStats.max, mean: queryStats.mean, n: queryStats.n },
    alertLagSec: {
      p50: lagStats.p50 == null ? null : Number((lagStats.p50 / 1000).toFixed(2)),
      p95: lagStats.p95 == null ? null : Number((lagStats.p95 / 1000).toFixed(2)),
      max: lagStats.max == null ? null : Number((lagStats.max / 1000).toFixed(2)),
      n: lagStats.n,
    },
    resources: {
      detection: lib.pickContainer(agg, "detection"),
      alert: lib.pickContainer(agg, "alert"),
      mongodb: lib.pickContainer(agg, "mongodb"),
      ingestion: lib.pickContainer(agg, "ingestion"),
    },
  };

  console.log("\n=== SUMMARY ===");
  console.dir(summary, { depth: 4 });

  lib.writeJson("plan2_results.json", summary);
  // Per-event CSVs for distribution plots.
  lib.writeCsv("plan2_detect_timings.csv", det.totalMs.map((t, i) => ({ i, totalMs: t, queryMs: det.queryMs[i] ?? "" })));
  lib.writeCsv("plan2_alert_lags.csv", alerts.map((a, i) => ({
    i,
    region: a.region,
    severity: a.severity,
    ffdi: a.triggeringData && a.triggeringData.ffdiScore,
    lagSec: igniteTime ? Number(((new Date(a.timestamp) - igniteTime) / 1000).toFixed(2)) : "",
  })));

  console.log(`\nWrote results to benchmarks/out/. Plot with:  python benchmarks/plot_plan2.py`);
  await lib.mongoose.disconnect();
  process.exit(0);
}

main().catch((e) => {
  console.error("plan2 failed:", e);
  process.exit(1);
});
