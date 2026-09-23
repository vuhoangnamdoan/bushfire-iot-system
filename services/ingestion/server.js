const express = require("express");
const { connectDB, Reading } = require("../../shared/db");
const { requireApiToken } = require("../../shared/auth");

const PORT = Number(process.env.PORT || 4001);

const app = express();
app.use(express.json({ limit: "1mb" }));

// Liveness probe is intentionally left OPEN (no token) for the load balancer.
app.get("/health", (req, res) => res.json({ status: "ok", service: "ingestion" }));

const os = require("os");
app.get("/whoami", (req, res) => res.json({ instance: os.hostname() }));

// Basic guard so a malformed message can't poison the collection.
function validateReading(body) {
  if (!body || typeof body !== "object") return "body must be a JSON object";
  if (!body.nodeId) return "nodeId is required";
  if (!body.region) return "region is required";
  const r = body.readings;
  if (!r || typeof r !== "object") return "readings object is required";
  const nums = ["temperature", "humidity", "windSpeed", "smokeLevel"];
  for (const k of nums) {
    if (r[k] != null && typeof r[k] !== "number") return `readings.${k} must be a number`;
  }
  return null;
}

// POST /api/sensor-data — write a single reading.
app.post("/api/sensor-data", requireApiToken(), async (req, res) => {
  const err = validateReading(req.body);
  if (err) return res.status(400).json({ error: err });

  try {
    const doc = await Reading.create({
      nodeId: req.body.nodeId,
      region: req.body.region,
      timestamp: req.body.timestamp ? new Date(req.body.timestamp) : new Date(),
      location: req.body.location,
      readings: req.body.readings,
    });
    res.status(201).json({ ok: true, id: doc._id });
  } catch (e) {
    console.error("[ingestion] write failed:", e.message);
    res.status(500).json({ error: "failed to store reading" });
  }
});

// GET /api/readings — historical query for operators / the detection service.
app.get("/api/readings", async (req, res) => {
  try {
    const { region, nodeId, from, to } = req.query;
    const limit = Math.min(Number(req.query.limit) || 100, 1000);

    const q = {};
    if (region) q.region = region;
    if (nodeId) q.nodeId = nodeId;
    if (from || to) {
      q.timestamp = {};
      if (from) q.timestamp.$gte = new Date(from);
      if (to) q.timestamp.$lte = new Date(to);
    }

    const docs = await Reading.find(q).sort({ timestamp: -1 }).limit(limit).lean();
    res.json({ count: docs.length, readings: docs });
  } catch (e) {
    console.error("[ingestion] query failed:", e.message);
    res.status(500).json({ error: "failed to query readings" });
  }
});

async function start() {
  await connectDB();
  app.listen(PORT, () => console.log(`[ingestion] listening on :${PORT}`));
}

start().catch((e) => {
  console.error("[ingestion] fatal:", e.message);
  process.exit(1);
});
