// bridge/server.js — MQTT -> Kafka (Redpanda) bridge.
//
// Replaces Node-RED on the critical throughput path. It does the minimum
// possible per message: read the topic to get the node id (the key), and hand
// the raw payload to Redpanda in batches. All parsing, validation, and storage
// moves downstream to the consumers, which scale horizontally. Redpanda then
// buffers bursts durably, so a slow consumer causes lag, not message loss —
// which is the whole point of the queue.

const mqtt = require("mqtt");
const { buildKafka, ensureTopic, TOPIC } = require("../../shared/kafka");

const MQTT_URL = process.env.MQTT_URL || "mqtt://mosquitto:1883";
const MQTT_TOPIC = process.env.MQTT_TOPIC || "+/+/readings";

// Batch tuning: flush when either limit is hit. Batching is essential for
// throughput — one Kafka round-trip per message would itself be a bottleneck.
const BATCH_MAX = Number(process.env.BRIDGE_BATCH_MAX || 500);
const BATCH_MS = Number(process.env.BRIDGE_BATCH_MS || 100);

const mqttOpts = {};
if (process.env.MQTT_USERNAME) mqttOpts.username = process.env.MQTT_USERNAME;
if (process.env.MQTT_PASSWORD) mqttOpts.password = process.env.MQTT_PASSWORD;

async function main() {
  const kafka = buildKafka("bridge");
  await ensureTopic(kafka);

  const producer = kafka.producer({ allowAutoTopicCreation: false });
  await producer.connect();
  console.log(`[bridge] connected to Redpanda, producing to "${TOPIC}"`);

  let batch = [];
  let flushing = false;
  let forwarded = 0;
  let lastReport = 0;

  async function flush() {
    if (flushing || batch.length === 0) return;
    flushing = true;
    const messages = batch;
    batch = [];
    try {
      await producer.send({ topic: TOPIC, messages });
      forwarded += messages.length;
    } catch (e) {
      // On produce failure, put the messages back so nothing is silently lost.
      batch = messages.concat(batch);
      console.error("[bridge] produce failed, will retry:", e.message);
    } finally {
      flushing = false;
    }
  }

  setInterval(flush, BATCH_MS);

  // Once-per-second forwarded-rate log, matching the fleet's msgs/sec output.
  setInterval(() => {
    const rate = forwarded - lastReport;
    lastReport = forwarded;
    console.log(`[bridge] forwarded/sec: ${rate}   total: ${forwarded}   pending: ${batch.length}`);
  }, 1000);

  const client = mqtt.connect(MQTT_URL, mqttOpts);
  client.on("connect", () => {
    console.log(`[bridge] connected to MQTT ${MQTT_URL}, subscribing ${MQTT_TOPIC}`);
    client.subscribe(MQTT_TOPIC, { qos: 0 }, (err) => {
      if (err) console.error("[bridge] subscribe failed:", err.message);
    });
  });

  client.on("message", (topic, payload) => {
    // topic = region/nodeId/readings -> key on nodeId for even partition spread.
    const parts = topic.split("/");
    const key = parts.length >= 2 ? parts[1] : null;
    batch.push({ key, value: payload }); // payload stays raw Buffer
    if (batch.length >= BATCH_MAX) flush();
  });

  client.on("error", (e) => console.error("[bridge] MQTT error:", e.message));

  const shutdown = async () => {
    console.log(`\n[bridge] stopping. Total forwarded: ${forwarded}`);
    try { await flush(); await producer.disconnect(); } catch (_) {}
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((e) => {
  console.error("[bridge] fatal:", e.message);
  process.exit(1);
});
