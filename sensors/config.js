const fs = require("fs");
const path = require("path");

// Security toggles
const tlsEnabled = String(process.env.TLS_ENABLED || "false").toLowerCase() === "true";

// Default broker URL follows the TLS flag unless MQTT_URL is set explicitly.
const defaultBrokerUrl = tlsEnabled 
  ? "mqtts://localhost:8883" 
  : "mqtt://localhost:1883";

// CA certificate used to verify the broker when TLS is on.
const caFile = process.env.MQTT_CA_FILE || path.resolve(__dirname, "../security/ca.crt");

// Build the options object passed to mqtt.connect()
function mqttOptions() {
  const opts = {};
  if (process.env.MQTT_USERNAME) opts.username = process.env.MQTT_USERNAME;
  if (process.env.MQTT_PASSWORD) opts.password = process.env.MQTT_PASSWORD;
  if (tlsEnabled) {
    try {
      opts.ca = fs.readFileSync(caFile);
    } catch (e) {
      console.error(`[config] TLS_ENABLED but CA file unreadable at ${caFile}: ${e.message}`);
      console.error("[config] run security/gen-certs.sh or set MQTT_CA_FILE.");
      throw e;
    }
    // Verify the broker's certificate chain against our CA (self-signed root).
    opts.rejectUnauthorized = true;
  }
  return opts;
}

module.exports = {
  // MQTT broker connection.
  brokerUrl: process.env.MQTT_URL || defaultBrokerUrl,

  // MQTT security.
  tlsEnabled,
  caFile,
  mqttOptions,

  // How often each node publishes a reading (real milliseconds).
  publishIntervalMs: Number(process.env.PUBLISH_INTERVAL_MS || 5000),

  // Spread of each node's FIRST publish across a window (ms). 0 = every node
  staggerMs:
    process.env.PUBLISH_STAGGER_MS != null ? Number(process.env.PUBLISH_STAGGER_MS) : 0,

  // Simulated-time speed-up. 1 = real time.
  timeScale: Number(process.env.TIME_SCALE || 1),

  // Ground-truth source
  dataSource: process.env.DATA_SOURCE || "synthetic",

  // Probability, per reading, that a sensor emits a faulty value (dropout, stuck, or spike).
  faultRate: Number(process.env.FAULT_RATE || 0.01),

  // Probability of ignition
  ignitionChance: Number(process.env.IGNITION_CHANCE || 0.0005),
};
