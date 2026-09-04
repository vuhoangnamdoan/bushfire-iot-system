const crypto = require("crypto");
const express = require("express");
const axios = require("axios");
const { connectDB, Alert } = require("../../shared/db");
const { requireApiToken } = require("../../shared/auth");

const PORT = Number(process.env.PORT || 4003);
const COOLDOWN_MS = Number(process.env.ALERT_COOLDOWN_MS || 5 * 60 * 1000); // 5 min
const DEFAULT_PARTIES = (process.env.NOTIFY_PARTIES || "fire_services,emergency_management")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// IFTTT Maker Webhooks (optional).
const IFTTT_KEY = process.env.IFTTT_KEY || "";
const IFTTT_EVENT = process.env.IFTTT_EVENT || "bushfire_alert";

const app = express();
app.use(express.json({ limit: "1mb" }));

app.get("/health", (req, res) => res.json({ status: "ok", service: "alert" }));

// Fire an IFTTT webhook if configured.
async function notifyIFTTT(alert) {
  if (!IFTTT_KEY) return { ifttt: "skipped (no IFTTT_KEY set)" };
  const url = `https://maker.ifttt.com/trigger/${IFTTT_EVENT}/with/key/${IFTTT_KEY}`;
  try {
    await axios.post(
      url,
      {
        value1: alert.region,
        value2: alert.severity,
        value3: `FFDI ${alert.triggeringData?.ffdiScore} | smoke ${alert.triggeringData?.smokeLevel}ppm | nodes ${(alert.triggeringNodes || []).join(", ")}`,
      },
      { timeout: 5000 }
    );
    return { ifttt: "sent" };
  } catch (e) {
    console.error("[alert] IFTTT notify failed:", e.message);
    return { ifttt: `failed: ${e.message}` };
  }
}

// POST /api/alert
app.post("/api/alert", requireApiToken(), async (req, res) => {
  const { region, severity, location, triggeringData, triggeringNodes } = req.body || {};
  if (!region || !severity) {
    return res.status(400).json({ error: "region and severity are required" });
  }

  try {
    // Cooldown: if an alert for this region was raised very recently, keep it.
    const since = new Date(Date.now() - COOLDOWN_MS);
    const existing = await Alert.findOne({
      region,
      status: "dispatched",
      timestamp: { $gte: since },
    })
      .sort({ timestamp: -1 })
      .lean();

    if (existing) {
      return res.json({ ok: true, deduplicated: true, alertId: existing.alertId });
    }

    const alertDoc = {
      alertId: crypto.randomUUID(),
      timestamp: new Date(),
      region,
      severity,
      location,
      triggeringData,
      triggeringNodes: triggeringNodes || [],
      status: "dispatched",
      notifiedParties: DEFAULT_PARTIES,
    };

    const saved = await Alert.create(alertDoc);

    // Notify: always log, optionally IFTTT.
    console.log(
      `[alert] ${severity.toUpperCase()} in ${region} | FFDI ${triggeringData?.ffdiScore} | ` +
        `notifying ${DEFAULT_PARTIES.join(", ")} | alertId ${saved.alertId}`
    );
    const notify = await notifyIFTTT(saved);

    res.status(201).json({ ok: true, alertId: saved.alertId, notify });
  } catch (e) {
    console.error("[alert] failed to create alert:", e.message);
    res.status(500).json({ error: "failed to create alert" });
  }
});

// GET /api/alerts — recent alerts, newest first.
app.get("/api/alerts", async (req, res) => {
  try {
    const { region } = req.query;
    const limit = Math.min(Number(req.query.limit) || 50, 500);
    const q = region ? { region } : {};
    const alerts = await Alert.find(q).sort({ timestamp: -1 }).limit(limit).lean();
    res.json({ count: alerts.length, alerts });
  } catch (e) {
    console.error("[alert] query failed:", e.message);
    res.status(500).json({ error: "failed to query alerts" });
  }
});

async function start() {
  await connectDB();
  app.listen(PORT, () =>
    console.log(`[alert] listening on :${PORT} | IFTTT ${IFTTT_KEY ? "enabled" : "disabled"}`)
  );
}

start().catch((e) => {
  console.error("[alert] fatal:", e.message);
  process.exit(1);
});
