# Scaling test harness

Two automated benchmarks that produce tables + charts for the project report.

- **Plan 1 — throughput ramp** (`plan1_throughput.js`): grows sensor count, finds the ingestion saturation knee.
- **Plan 2 — event-intensity burst** (`plan2_burst.js`): ignites many regions at once, stresses detection + alert, verifies the cooldown.

Outputs land in `benchmarks/out/` (JSON + CSV + PNG + `*_table.md`).

## Prerequisites

1. The Docker stack running: `docker compose up -d` (from repo root).
2. `shared/` deps installed once: `cd shared && npm install` (gives the harness Mongoose).
3. For charts: `pip install matplotlib` (already present if `plot_*` runs).
4. `docker` must be on your PATH (it is in your normal terminal). Override with `DOCKER_BIN=/path/to/docker` if needed.

## Plan 1 — throughput ramp

```bash
# Full run from the proposal: 10/50/100/200/500 nodes, 5 min each, 10s interval
cd benchmarks && STEPS=10,50,100,200,500 HOLD_SEC=300 INTERVAL_MS=10000 node plan1_throughput.js
```
```bash
python3 plot_plan1.py     # writes charts + plan1_table.md
```

**Columns / metrics**

| Field | Meaning |
|---|---|
| `published` / `stored` | messages sent by the fleet vs rows in `readings` |
| `lossPct` | `(published - stored) / published` — should be ~0 until saturation |
| `latP50ms` / `latP95ms` / `latMaxMs` | end-to-end ingestion latency = `createdAt - reading.timestamp` (includes MQTT + Node-RED transit), via Mongo `$percentile` |
| `ingCpuAvg/Max`, `mongoCpuAvg/Max`, `*MemAvgMB` | container CPU%/memory from `docker stats` |

## Plan 2 — event-intensity burst

```bash
cd benchmarks && NODES=100 BASELINE_SEC=30 SPIKE_SEC=120 INTERVAL_MS=5000 node plan2_burst.js
```
```bash
python3 plot_plan2.py     # writes charts + plan2_table.md
```

Runs a calm baseline, then a spike where **all nodes across 5 regions ignite at once** (`FORCE_FIRE=1`). Node-RED routes the surge of over-threshold readings to `/api/detect`.

**Metrics** (from the detection service's `[metrics]` logs + the `alerts` collection)

| Field | Meaning |
|---|---|
| `detectCalls` / `detectTriggered` | `/api/detect` calls during the burst, and how many crossed threshold |
| `alertsWritten` / `regionsAlerted` | rows added to `alerts`, and distinct regions |
| `cooldownSuppressed` | triggered detects that did **not** create a new alert (cooldown working) |
| `detectTotalMs` p50/p95/max | detection response time |
| `mongoReadMs` p50/p95/max | recent-window regional query latency (the likely bottleneck) |
| `alertLagSec` p50/p95/max | ignition → alert dispatched (includes up to one publish interval) |

Keep `NODES * (1000/INTERVAL_MS)` close to a Plan 1 step so you compare intensity at equal volume. Success criterion: alerts dispatched within a few seconds, no dropped detections, and `alertsWritten ≈ regions` (not hundreds) proving the cooldown held.

## Notes / caveats

- Latency uses the sensor `timestamp` (host clock) and DB `createdAt` (container clock). On Docker Desktop these track the host closely; small skew is possible.
- Plan 1 fixes ingestion at one instance to expose its true limit. To show horizontal scaling, run `docker compose up -d --scale ingestion=3` behind a load balancer and re-run (AWS/ECS work).
- `out/` currently may contain data from a quick smoke run — real runs overwrite it.
