// detection/consumer.js — Kafka consumer for the detection tier.
//
// Replaces the Node-RED "route if risky" branch. It consumes the same
// "sensor-readings" topic in its OWN consumer group ("detection"), so it runs
// in parallel with the ingestion group over the same stream — the classic
// pub/sub fan-out that a message queue enables. It applies the same smoke
// pre-filter the Node-RED flow used, then runs the existing FFDI rule and
// forwards confirmed events to the alert service.

const axios = require("axios");
const { buildKafka, ensureTopic, TOPIC } = require("../../shared/kafka");
const { Reading } = require("../../shared/db");
const { authHeaders } = require("../../shared/auth");
const { cleanReading } = require("../../shared/processing");
const rule = require("./rule");

const GROUP = process.env.KAFKA_GROUP_DETECTION || "detection";
const ALERT_URL = process.env.ALERT_URL || "http://alert:4003/api/alert";
const RECENT_WINDOW_MIN = Number(process.env.RECENT_WINDOW_MIN || 10);
// Same threshold the Node-RED "route if risky" node used to pre-filter.
const SMOKE_ELEVATED = Number(process.env.SMOKE_ELEVATED_PPM || 150);

function worstCase(readings) {
  return {
    temperature: Math.max(...readings.map((r) => r.readings?.temperature ?? -Infinity)),
    humidity: Math.min(...readings.map((r) => r.readings?.humidity ?? Infinity)),
    windSpeed: Math.max(...readings.map((r) => r.readings?.windSpeed ?? -Infinity)),
    smokeLevel: Math.max(...readings.map((r) => r.readings?.smokeLevel ?? -Infinity)),
  };
}

async function detect(reading) {
  const t0 = Date.now();
  const since = new Date(Date.now() - RECENT_WINDOW_MIN * 60 * 1000);
  const q0 = Date.now();
  const recent = await Reading.find({ region: reading.region, timestamp: { $gte: since } })
    .sort({ timestamp: -1 })
    .limit(500)
    .lean();
  const queryMs = Date.now() - q0;

  const sample = [...recent, reading];
  const snapshot = worstCase(sample);
  const decision = rule.evaluate(snapshot, {});

  if (!decision.triggered) {
    console.log(`[metrics] detect totalMs=${Date.now() - t0} queryMs=${queryMs} recent=${recent.length} triggered=false region=${reading.region}`);
    return;
  }

  const triggeringNodes = [
    ...new Set(
      sample
        .filter((r) => r.readings && rule.evaluate(r.readings).triggered)
        .map((r) => r.nodeId)
        .filter(Boolean)
    ),
  ];
  if (triggeringNodes.length === 0 && reading.nodeId) triggeringNodes.push(reading.nodeId);

  const alertPayload = {
    region: reading.region,
    severity: decision.severity,
    location: reading.location,
    triggeringData: { ...snapshot, ffdiScore: decision.ffdiScore },
    triggeringNodes,
    reasons: decision.reasons,
  };

  let dedup = 0;
  try {
    const resp = await axios.post(ALERT_URL, alertPayload, { timeout: 5000, headers: authHeaders() });
    dedup = resp.data && resp.data.deduplicated ? 1 : 0;
  } catch (e) {
    console.error("[detection] failed to reach alert service:", e.message);
  }
  console.log(`[metrics] detect totalMs=${Date.now() - t0} queryMs=${queryMs} recent=${recent.length} triggered=true region=${reading.region} dedup=${dedup}`);
}

async function startConsumer() {
  const kafka = buildKafka(`detection-${process.pid}`);
  await ensureTopic(kafka);

  const consumer = kafka.consumer({ groupId: GROUP, sessionTimeout: 30000, heartbeatInterval: 3000 });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });
  console.log(`[detection] consuming "${TOPIC}" in group "${GROUP}" (smoke pre-filter >= ${SMOKE_ELEVATED}ppm)`);

  await consumer.run({
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;
        const clean = cleanReading(message.value);
        resolveOffset(message.offset);
        if (!clean.ok) continue;
        const r = clean.reading;
        // Pre-filter: only readings with elevated smoke reach the detector.
        if ((r.readings?.smokeLevel || 0) < SMOKE_ELEVATED) continue;
        try {
          await detect(r);
        } catch (e) {
          console.error("[detection] detect failed:", e.message);
        }
      }
      await heartbeat();
    },
  });

  const shutdown = async () => {
    console.log("\n[detection] consumer stopping.");
    try { await consumer.disconnect(); } catch (_) {}
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
  return consumer;
}

module.exports = { startConsumer };
