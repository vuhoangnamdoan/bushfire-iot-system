const express = require("express");
const axios = require("axios");
const { connectDB, Reading } = require("../../shared/db");
const { requireApiToken, authHeaders } = require("../../shared/auth");
const rule = require("./rule");

const PORT = Number(process.env.PORT || 4002);
const ALERT_URL = process.env.ALERT_URL || "http://localhost:4003/api/alert";
const RECENT_WINDOW_MIN = Number(process.env.RECENT_WINDOW_MIN || 10);

const app = express();
app.use(express.json({ limit: "1mb" }));

app.get("/health", (req, res) => res.json({ status: "ok", service: "detection" }));

// Placeholder for the future deep-learning forecast.
async function forecast(/* recentReadings */) {
  return null; // e.g. { predictedFFDI, horizonMin, confidence }
}

// Combine several readings into the worst-case snapshot the rule scores against.
function worstCase(readings) {
  return {
    temperature: Math.max(...readings.map((r) => r.readings?.temperature ?? -Infinity)),
    humidity: Math.min(...readings.map((r) => r.readings?.humidity ?? Infinity)),
    windSpeed: Math.max(...readings.map((r) => r.readings?.windSpeed ?? -Infinity)),
    smokeLevel: Math.max(...readings.map((r) => r.readings?.smokeLevel ?? -Infinity)),
  };
}

// POST /api/detect - body is a cleaned reading
app.post("/api/detect", requireApiToken(), async (req, res) => {
  const t0 = Date.now(); // for [metrics] timing (scaling-test evidence)
  const reading = req.body;
  if (!reading || !reading.region || !reading.readings) {
    return res.status(400).json({ error: "reading with region and readings is required" });
  }

  try {
    // Pull recent readings for this region to corroborate the event.
    const since = new Date(Date.now() - RECENT_WINDOW_MIN * 60 * 1000);
    const q0 = Date.now();
    const recent = await Reading.find({ region: reading.region, timestamp: { $gte: since } })
      .sort({ timestamp: -1 })
      .limit(500)
      .lean();
    const queryMs = Date.now() - q0; // recent-window read latency

    // Always include the incoming reading: Node-RED calls ingestion and detection in parallel
    const sample = [...recent, reading];
    const snapshot = worstCase(sample);

    const decision = rule.evaluate(snapshot, {});
    await forecast(sample); // stub; result not yet used

    if (!decision.triggered) {
      console.log(
        `[metrics] detect totalMs=${Date.now() - t0} queryMs=${queryMs} ` +
          `recent=${recent.length} triggered=false region=${reading.region}`
      );
      return res.json({ triggered: false, ...decision });
    }

    // Which nodes are individually showing risky conditions right now.
    const triggeringNodes = [
      ...new Set(
        sample
          .filter((r) => r.readings && rule.evaluate(r.readings).triggered)
          .map((r) => r.nodeId)
          .filter(Boolean)
      ),
    ];
    // Fall back to at least the reading's own node.
    if (triggeringNodes.length === 0 && reading.nodeId) triggeringNodes.push(reading.nodeId);

    const alertPayload = {
      region: reading.region,
      severity: decision.severity,
      location: reading.location,
      triggeringData: { ...snapshot, ffdiScore: decision.ffdiScore },
      triggeringNodes,
      reasons: decision.reasons,
    };

    // Forward to the alert service. Detection's job ends once handed off.
    let alertResult = null;
    try {
      // Attach the bearer token so the (protected) alert service accepts us.
      const resp = await axios.post(ALERT_URL, alertPayload, {
        timeout: 5000,
        headers: authHeaders(),
      });
      alertResult = resp.data;
    } catch (e) {
      console.error("[detection] failed to reach alert service:", e.message);
    }

    console.log(
      `[metrics] detect totalMs=${Date.now() - t0} queryMs=${queryMs} ` +
        `recent=${recent.length} triggered=true region=${reading.region} ` +
        `dedup=${alertResult && alertResult.deduplicated ? 1 : 0}`
    );
    res.json({ triggered: true, ...decision, triggeringNodes, alert: alertResult });
  } catch (e) {
    console.error("[detection] detect failed:", e.message);
    res.status(500).json({ error: "detection failed" });
  }
});

const KAFKA_ENABLED = String(process.env.KAFKA_ENABLED || "false").toLowerCase() === "true";

async function start() {
  await connectDB();
  app.listen(PORT, () => console.log(`[detection] listening on :${PORT} (alert -> ${ALERT_URL})`));

  if (KAFKA_ENABLED) {
    const { startConsumer } = require("./consumer");
    await startConsumer();
  } else {
    console.log("[detection] KAFKA_ENABLED=false — HTTP /api/detect path only (legacy/Node-RED mode)");
  }
}

start().catch((e) => {
  console.error("[detection] fatal:", e.message);
  process.exit(1);
});
