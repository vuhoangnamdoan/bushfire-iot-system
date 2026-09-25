// processing.js — reading clean/validate/format logic.
//
// This is a faithful port of the two Node-RED function nodes ("clean & validate"
// and "format") that used to run on the single-threaded processing tier. Moving
// it here lets each Kafka consumer run it in parallel, so the work that once
// bottlenecked in one Node-RED process now scales across consumer instances.
//
// Pure and dependency-free so it can be unit-tested without a broker or DB.

// Physically valid range per sensor. Anything outside is a dropout/stuck/spike
// fault and the reading is dropped (same ranges the Node-RED flow used).
const RANGES = {
  temperature: [-10, 80],
  humidity: [0, 100],
  windSpeed: [0, 150],
  smokeLevel: [0, 2000],
};

// Parse + validate a raw payload (Buffer, string, or object).
// Returns { ok:true, reading } or { ok:false, reason }.
function cleanReading(raw) {
  let data;
  try {
    if (Buffer.isBuffer(raw)) raw = raw.toString("utf8");
    data = typeof raw === "string" ? JSON.parse(raw) : raw;
  } catch (e) {
    return { ok: false, reason: "invalid_json" };
  }

  if (!data || !data.nodeId || !data.region || !data.readings) {
    return { ok: false, reason: "missing_fields" };
  }

  for (const k of Object.keys(RANGES)) {
    const [lo, hi] = RANGES[k];
    const v = data.readings[k];
    if (typeof v !== "number" || !isFinite(v) || v < lo || v > hi) {
      return { ok: false, reason: "invalid_" + k };
    }
  }

  return { ok: true, reading: data };
}

// Standardise structure and timestamp before storing (the Node-RED "format" step).
function formatReading(d) {
  return {
    nodeId: d.nodeId,
    region: d.region,
    timestamp: d.timestamp ? new Date(d.timestamp) : new Date(),
    location: d.location || null,
    readings: {
      temperature: d.readings.temperature,
      humidity: d.readings.humidity,
      windSpeed: d.readings.windSpeed,
      smokeLevel: d.readings.smokeLevel,
    },
  };
}

// Small time-window de-duplicator (same node + timestamp seen recently), the
// equivalent of the Node-RED flow's `seen` map. Kept per consumer; because
// messages are keyed by nodeId, a node's readings always land on the same
// partition and therefore the same consumer, so this stays correct when scaled.
function createDedup(ttlMs = 60000) {
  const seen = new Map();
  return function isDuplicate(nodeId, timestamp) {
    const key = nodeId + "|" + timestamp;
    const now = Date.now();
    if (seen.has(key)) return true;
    seen.set(key, now);
    // opportunistic cleanup
    if (seen.size > 5000) {
      for (const [k, t] of seen) if (now - t > ttlMs) seen.delete(k);
    }
    return false;
  };
}

module.exports = { cleanReading, formatReading, createDedup, RANGES };
