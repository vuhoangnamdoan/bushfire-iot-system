// plan1_throughput.js — Sensor-count throughput test (steady load ramp).
//
// Ramps the sensor fleet through increasing node counts, holding each step, and
// records: published vs stored rate (loss), end-to-end ingestion latency
// (p50/p95/max), and ingestion/mongodb CPU+memory. Finds the saturation knee.
//
// Usage (from benchmarks/):
//   node plan1_throughput.js
//   STEPS=10,50,100,200,500 HOLD_SEC=300 INTERVAL_MS=10000 node plan1_throughput.js
//   STEPS=10,50,100 HOLD_SEC=30 node plan1_throughput.js      # quick smoke run
//
// Prereqs: docker stack up (docker compose up -d). Fault injection is disabled
// (FAULT_RATE=0) so any published-vs-stored gap is real pipeline loss, and
// TIME_SCALE=1 / IGNITION_CHANCE=0 keep the load steady with no fire branch.

const lib = require("./lib");

const STEPS = (process.env.STEPS || "10,50,100,200,500").split(",").map((s) => Number(s.trim()));
const HOLD_SEC = Number(process.env.HOLD_SEC || 300);
const INTERVAL_MS = Number(process.env.INTERVAL_MS || 10000);
const DRAIN_SEC = Number(process.env.DRAIN_SEC || 3);

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

  const sampler = lib.startDockerStats(["ingestion", "mongodb", "node-red", "mosquitto"]);
  const windowStart = new Date();
  await lib.sleep(HOLD_SEC * 1000);

  await lib.stopFleet(child);
  await lib.sleep(DRAIN_SEC * 1000); // let in-flight messages land
  const windowEnd = new Date();
  const agg = sampler.stop();

  const lat = await lib.ingestionLatency(windowStart, windowEnd);
  const ing = lib.pickContainer(agg, "ingestion");
  const mon = lib.pickContainer(agg, "mongodb");
  const nr = lib.pickContainer(agg, "node-red");
  const mq = lib.pickContainer(agg, "mosquitto");

  // The component with the highest peak CPU is the likely saturation point.
  const cpuByComponent = {
    ingestion: ing.cpuMax,
    mongodb: mon.cpuMax,
    "node-red": nr.cpuMax,
    mosquitto: mq.cpuMax,
  };
  const bottleneck = Object.entries(cpuByComponent)
    .filter(([, v]) => v != null)
    .sort((a, b) => b[1] - a[1])[0];

  const durationSec = (windowEnd - windowStart) / 1000;
  const row = {
    nodes,
    targetMsgS: Number((nodes / (INTERVAL_MS / 1000)).toFixed(2)),
    published,
    stored: lat.count,
    publishedRateMsgS: Number((published / durationSec).toFixed(2)),
    storedRateMsgS: Number((lat.count / durationSec).toFixed(2)),
    lossPct: published ? Number((((published - lat.count) / published) * 100).toFixed(2)) : 0,
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
    nodeRedCpuAvg: nr.cpuAvg,
    nodeRedCpuMax: nr.cpuMax,
    nodeRedMemAvgMB: nr.memAvgMB,
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
  const jf = lib.writeJson("plan1_results.json", { config: { STEPS, HOLD_SEC, INTERVAL_MS }, rows });
  const cf = lib.writeCsv("plan1_results.csv", rows);
  console.log(`\nWrote:\n  ${jf}\n  ${cf}`);
  console.log("Plot with:  python benchmarks/plot_plan1.py");
  await lib.mongoose.disconnect();
  process.exit(0);
}

main().catch((e) => {
  console.error("plan1 failed:", e);
  process.exit(1);
});
