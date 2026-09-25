// plan3_scaling.js — horizontal-scaling test for the ingestion consumer group.
//
// Runs the same load ramp as plan1_queue, but is meant to be run once per
// consumer-group size (1, 2, 3 ingestion replicas). It tags its output file with
// the SCALE it was told it is running at, so plot_scaling.py can compare the
// sustained no-loss rate across group sizes and show horizontal scalability
// (the Theodolite idea: does adding resources sustain proportionally more load).
//
// This harness does NOT change the replica count itself — you scale the group
// with docker compose first (see the README HD section), then run this with the
// matching SCALE. Throughput is measured from MongoDB row counts, which are
// global across all replicas, so the measurement is correct for any group size.
//
// Usage (from benchmarks/), with the queue stack up and scaled to N first:
//   SCALE=1 node plan3_scaling.js
//   SCALE=2 node plan3_scaling.js
//   SCALE=3 node plan3_scaling.js
// Defaults: STEPS give 1000/2000/3000/4000 msg/s at INTERVAL_MS=5000.

const lib = require("./lib");
const { Reading } = require("../shared/db");

const SCALE = Number(process.env.SCALE || 1);
const STEPS = (process.env.STEPS || "5000,10000,15000,20000").split(",").map((s) => Number(s.trim()));
const HOLD_SEC = Number(process.env.HOLD_SEC || 60);
const INTERVAL_MS = Number(process.env.INTERVAL_MS || 5000);
const STABLE_SEC = Number(process.env.DRAIN_STABLE_SEC || 6);
const DRAIN_MAX_SEC = Number(process.env.DRAIN_MAX_SEC || 180);
const CONTAINERS = ["ingestion", "mongodb", "redpanda", "bridge"];

async function drainUntilStable() {
  let last = -1, stableFor = 0;
  const t0 = Date.now();
  while ((Date.now() - t0) / 1000 < DRAIN_MAX_SEC) {
    const count = await Reading.estimatedDocumentCount();
    if (count === last) { stableFor += 2; if (stableFor >= STABLE_SEC) break; }
    else { stableFor = 0; last = count; }
    await lib.sleep(2000);
  }
  return last;
}

async function runStep(nodes) {
  console.log(`\n=== SCALE ${SCALE} | STEP ${nodes} nodes (~${(nodes / (INTERVAL_MS / 1000)).toFixed(0)} msg/s), hold ${HOLD_SEC}s ===`);
  await lib.resetDb();

  let published = 0;
  const child = lib.spawnFleet(
    { NODES: String(nodes), PUBLISH_INTERVAL_MS: String(INTERVAL_MS), PUBLISH_STAGGER_MS: String(INTERVAL_MS),
      IGNITION_CHANCE: "0", FAULT_RATE: "0", TIME_SCALE: "1", REGIONS: "5" },
    (line) => {
      const m = line.match(/total:\s*(\d+)/); if (m) published = Number(m[1]);
      const d = line.match(/Total published:\s*(\d+)/); if (d) published = Number(d[1]);
    }
  );

  const sampler = lib.startDockerStats(CONTAINERS);
  const windowStart = new Date();
  await lib.sleep(HOLD_SEC * 1000);
  await lib.stopFleet(child);
  const storedFinal = await drainUntilStable();
  const windowEnd = new Date();
  const agg = sampler.stop();

  const lat = await lib.ingestionLatency(windowStart, windowEnd);
  const ing = lib.pickContainer(agg, "ingestion");
  const stored = lat.count || storedFinal;

  const row = {
    scale: SCALE,
    nodes,
    targetMsgS: Number((nodes / (INTERVAL_MS / 1000)).toFixed(2)),
    published,
    stored,
    storedRateMsgS: Number((stored / HOLD_SEC).toFixed(2)),
    lossPct: published ? Number((((published - stored) / published) * 100).toFixed(2)) : 0,
    latP50ms: lat.p50, latP95ms: lat.p95, latMaxMs: lat.max,
    ingCpuMax: ing.cpuMax,
  };
  console.table([row]);
  return row;
}

async function main() {
  await lib.connectDB();
  const rows = [];
  for (const n of STEPS) rows.push(await runStep(n));
  const jf = lib.writeJson(`plan_scaling_${SCALE}.json`, { config: { SCALE, STEPS, HOLD_SEC, INTERVAL_MS }, rows });
  const cf = lib.writeCsv(`plan_scaling_${SCALE}.csv`, rows);
  console.log(`\nWrote:\n  ${jf}\n  ${cf}`);
  console.log(`Run SCALE=1,2,3 (scaling the stack each time), then: python3 plot_scaling.py`);
  await lib.mongoose.disconnect();
  process.exit(0);
}

main().catch((e) => { console.error("plan3_scaling failed:", e); process.exit(1); });
