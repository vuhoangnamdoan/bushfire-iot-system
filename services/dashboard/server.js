const fs = require("fs");
const path = require("path");
const http = require("http");
const express = require("express");
const axios = require("axios");
const mqtt = require("mqtt");
const { Server } = require("socket.io");
const { evaluate } = require("../detection/rule");

const PORT = Number(process.env.PORT || 4004);
const MQTT_URL = process.env.MQTT_URL || "mqtt://mosquitto:1883";
const INGESTION_URL = process.env.INGESTION_URL || "http://ingestion:4001";
const TOPIC = process.env.MQTT_TOPIC || "+/+/readings";

// MQTT security
const TLS_ENABLED = String(process.env.TLS_ENABLED || "false").toLowerCase() === "true";
const MQTT_CA_FILE = process.env.MQTT_CA_FILE || "/security/ca.crt";

function mqttOptions() {
  const opts = {};
  if (process.env.MQTT_USERNAME) opts.username = process.env.MQTT_USERNAME;
  if (process.env.MQTT_PASSWORD) opts.password = process.env.MQTT_PASSWORD;
  if (TLS_ENABLED) {
    opts.ca = fs.readFileSync(MQTT_CA_FILE);
    opts.rejectUnauthorized = true;
  }
  return opts;
}

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// nodeId -> latest enriched reading. Rebuilt from live MQTT traffic.
const latestByNode = new Map();

// Enrich a raw reading with FFDI + severity, mirroring the detection service's scoring
function enrich(reading) {
  const r = reading.readings || {};
  const { ffdiScore, severity, band } = evaluate(r);
  return { ...reading, ffdiScore, severity, band };
}

// MQTT ingest
const client = mqtt.connect(MQTT_URL, mqttOptions());

client.on("connect", () => {
  console.log(`[dashboard] MQTT connected ${MQTT_URL}`);
  client.subscribe(TOPIC, (err) => {
    if (err) console.error("[dashboard] subscribe failed:", err.message);
    else console.log(`[dashboard] subscribed ${TOPIC}`);
  });
});

client.on("error", (e) => console.error("[dashboard] MQTT error:", e.message));

client.on("message", (topic, payload) => {
  let reading;
  try {
    reading = JSON.parse(payload.toString());
  } catch (e) {
    console.error(`[dashboard] bad payload on ${topic}:`, e.message);
    return;
  }
  if (!reading || !reading.nodeId) return;

  const enriched = enrich(reading);
  latestByNode.set(enriched.nodeId, enriched);
  io.emit("reading", enriched);
});

// ---- HTTP -----------------------------------------------------------------
app.get("/health", (req, res) => res.json({ status: "ok", service: "dashboard" }));

// Latest enriched reading per node, for the first paint of the map.
app.get("/api/nodes", (req, res) => {
  res.json(Array.from(latestByNode.values()));
});

// Seed a node's chart with recent history from the ingestion service.
app.get("/api/history", async (req, res) => {
  const { nodeId } = req.query;
  if (!nodeId) return res.status(400).json({ error: "nodeId is required" });

  try {
    const url = `${INGESTION_URL}/api/readings`;
    const { data } = await axios.get(url, {
      params: { nodeId, limit: 100 },
      timeout: 5000,
    });
    // Ingestion returns { count, readings } sorted newest-first. Enrich each so
    // the chart has ffdiScore, and hand back oldest-first for plotting.
    const readings = (data.readings || [])
      .map(enrich)
      .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    res.json({ count: readings.length, readings });
  } catch (e) {
    console.error("[dashboard] history proxy failed:", e.message);
    res.status(502).json({ error: "failed to fetch history from ingestion" });
  }
});

app.use(express.static(path.join(__dirname, "public")));

io.on("connection", (socket) => {
  console.log(`[dashboard] client connected (${io.engine.clientsCount} total)`);
  socket.on("disconnect", () =>
    console.log(`[dashboard] client disconnected (${io.engine.clientsCount} total)`)
  );
});

server.listen(PORT, () => console.log(`[dashboard] listening on :${PORT}`));
