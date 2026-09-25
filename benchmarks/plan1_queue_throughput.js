// plan1_queue_throughput.js — Sensor-count throughput test for the QUEUE pipeline.
//
// Same load ramp as plan1_throughput.js (the Node-RED "before"), but for the
// Redpanda queue pipeline (the "after"). It records the same metrics so the two
// runs are directly comparable: published vs stored rate (loss), end-to-end
// ingestion latency (p50/p95/max), and per-container CPU+memory.
//
// The one difference from plan1: after the fleet stops, this harness DRAINS the
// queue until the stored count stops rising. That is the whole point of the
// queue — a burst is buffered by Redpanda and stored eventually (as lag), not
// dropped (as loss). Cutting off too early would falsely report loss.
//
// Usage (from benchmarks/), with the queue stack already up:
//   COMPOSE_PROFILES=queue KAFKA_ENABLED=true docker compose up -d --build
//   cd benchmarks
//   STEPS=300,500,1000,5000,10000 HOLD_SEC=300 INTERVAL_MS=5000 node plan1_queue_throughput.js
//   python3 plot_plan1.py            # (points at plan1_results.json — see plot note)

const lib = require("./lib");
const { Reading } = require("../shared/db");

const STEPS = (process.env.STEPS || "300,500,1000,5000,10000").split(",").map((s) => Number(s.trim()));
const HOLD_SEC = Number(process.env.HOLD_SEC || 300);
const INTERVAL_MS = Number(process.env.INTERVAL_MS || 5000);
// Drain settings: keep counting stored docs until the count is stable for
// STABLE_SEC, or until DRAIN_MAX_SEC elapses (safety cap for huge backlogs).
const STABLE_SEC = Number(process.env.DRAIN_STABLE_SEC || 8);
const DRAIN_MAX_SEC = Number(process.env.DRAIN_MAX_SEC || 300);
const CONTAINERS = ["ingestion", "mongodb", "redpanda", "bridge", "mosquitto"];

async function drainUntilStable() {
  let last = -1;
  let stableFor = 0;
  const t0 = Date.now();
  while ((Date.now() - t0) / 1000 < DRAIN_MAX_SEC) {
    const count = await Reading.estimatedDocumentCount();
    if (count === last) {
      stableFor += 2;
      if (stableFor >= STABLE_SEC) break;
    } else {
      stableFor = 0;
      last = count;
    }
    await lib.sleep(2000);
  }
  return last;
}

async function runStep(nodes) {
  console.log(`\n=== STEP: ${nodes} nodes (target ~${(nodes / (INTERVAL_MS / 1000)).toFixed(1)} msg/s), hold ${HOLD_SEC}s ===`);
  await lib.resetDb();

  let published = 0;
  const child = lib.spawnFleet(
    {
      NODES: String(nodes),
      PUBLISH_INTERVAL_MS: String(INTERVAL_MS),
      PUBLISH_STAGGER_MS: String(INTERVAL_MS), // steady rate, not a burst
      IGNITION_CHANCE: "0",
      FAULT_RATE: "0",
      TIME_SCALE: "1",
      REGIONS: "5",
    },
    (line) => {
      const m = line.match(/total:\s*(\d+)/);
      if (m) published = Number(m[1]);
      const done = line.match(/Total published:\s*(\d+)/);
      if (done) published = Number(done[1]);
    }
  );

  const sampler = lib.startDockerStats(CONTAINERS);
  const windowStart = new Date();
  await lib.sleep(HOLD_SEC * 1000);

  await lib.stopFleet(child);
  // Drain the queue: wait for the consumer to catch up before measuring.
  console.log("[drain] waiting for the queue to drain into MongoDB...");
  const storedFinal = await drainUntilStable();
  const windowEnd = new Date();
  const agg = sampler.stop();

  const lat = await lib.ingestionLatency(windowStart, windowEnd);
  const ing = lib.pickContainer(agg, "ingestion");
  const mon = lib.pickContainer(agg, "mongodb");
  const rp = lib.pickContainer(agg, "redpanda");
  const br = lib.pickContainer(agg, "bridge");
  const mq = lib.pickContainer(agg, "mosquitto");

  const cpuByComponent = {
    ingestion: ing.cpuMax,
    mongodb: mon.cpuMax,
    redpanda: rp.cpuMax,
    bridge: br.cpuMax,
    mosquitto: mq.cpuMax,
  };
  const bottleneck = Object.entries(cpuByComponent)
    .filter(([, v]) => v != null)
    .sort((a, b) => b[1] - a[1])[0];

  const durationSec = HOLD_SEC; // offered-load window (drain excluded from rate)
  const stored = lat.count || storedFinal;
  const row = {
    nodes,
    targetMsgS: Number((nodes / (INTERVAL_MS / 1000)).toFixed(2)),
    published,
    stored,
    publishedRateMsgS: Number((published / durationSec).toFixed(2)),
    storedRateMsgS: Number((stored / durationSec).toFixed(2)),
    lossPct: published ? Number((((published - stored) / published) * 100).toFixed(2)) : 0,
    latP50ms: lat.p50,
    latP95ms: lat.p95,
    latMaxMs: lat.max,
    latMeanMs: lat.mean,
    ingCpuAvg: ing.cpuAvg,
    ingCpuMax: ing.cpuMax,
    ingMemAvgMB: ing.memAvgMB,
    mongoCpuAvg: mon.cpuAvg,
    mongoCpuMax: mon.cpuMax,
    mongoMemAvgMB: mon.memAvgMB,
    redpandaCpuAvg: rp.cpuAvg,
    redpandaCpuMax: rp.cpuMax,
    redpandaMemAvgMB: rp.memAvgMB,
    bridgeCpuAvg: br.cpuAvg,
    bridgeCpuMax: br.cpuMax,
    bridgeMemAvgMB: br.memAvgMB,
    mosqCpuAvg: mq.cpuAvg,
    mosqCpuMax: mq.cpuMax,
    mosqMemAvgMB: mq.memAvgMB,
    bottleneck: bottleneck ? `${bottleneck[0]} (${bottleneck[1]}%)` : null,
  };
  console.table([row]);
  return row;
}

async function main() {
  await lib.connectDB();
  const rows = [];
  for (const n of STEPS) {
    rows.push(await runStep(n));
  }
  const jf = lib.writeJson("plan1_queue_results.json", { config: { STEPS, HOLD_SEC, INTERVAL_MS, pipeline: "queue" }, rows });
  const cf = lib.writeCsv("plan1_queue_results.csv", rows);
  console.log(`\nWrote:\n  ${jf}\n  ${cf}`);
  await lib.mongoose.disconnect();
  process.exit(0);
}

main().catch((e) => {
  console.error("plan1_queue failed:", e);
  process.exit(1);
});
