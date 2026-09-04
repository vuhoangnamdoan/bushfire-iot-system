const mqtt = require("mqtt");
const config = require("./config");
const { createDataSource } = require("./dataGenerator");

function randn() {
  let u = 0, v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}
function clamp(x, lo, hi) { return Math.max(lo, Math.min(hi, x)); }

// One physical sensor
class Sensor {
  constructor({ key, unit, noise, min, max }) {
    Object.assign(this, { key, unit, noise, min, max });
    this.last = null;
  }
  measure(truth) {
    if (Math.random() < config.faultRate) {
      const kind = Math.random();
      if (kind < 0.34) return null; // dropout
      if (kind < 0.67 && this.last !== null) return this.last; // stuck value
      return Number((this.max * (1.5 + Math.random())).toFixed(1)); // impossible spike
    }
    const value = clamp(truth + randn() * this.noise, this.min, this.max);
    this.last = Number(value.toFixed(1));
    return this.last;
  }
}

class SensorNode {
  constructor(cfg) {
    Object.assign(this, cfg); // nodeId, region, location, base*, source, client
    this.topic = `${this.region}/${this.nodeId}/readings`;

    // The five sensors of this node.
    this.sensors = {
      temperature: new Sensor({ key: "temperature", unit: "C",   noise: 0.5, min: -10, max: 80 }),
      humidity:    new Sensor({ key: "humidity",    unit: "%",   noise: 1.0, min: 0,   max: 100 }),
      windSpeed:   new Sensor({ key: "windSpeed",   unit: "kmh", noise: 1.5, min: 0,   max: 150 }),
      smokeLevel:  new Sensor({ key: "smokeLevel",  unit: "ppm", noise: 5.0, min: 0,   max: 2000 }),
    };
  }

  readGps() {
    // Fixed position with a few metres of variation, like a real GPS module.
    return {
      latitude: Number((this.location.latitude + randn() * 0.00003).toFixed(6)),
      longitude: Number((this.location.longitude + randn() * 0.00003).toFixed(6)),
    };
  }

  buildPayload() {
    const truth = this.source.sample(this, Date.now());
    return {
      nodeId: this.nodeId,
      region: this.region,
      timestamp: new Date().toISOString(),
      location: this.readGps(),
      readings: {
        temperature: this.sensors.temperature.measure(truth.temperature),
        humidity:    this.sensors.humidity.measure(truth.humidity),
        windSpeed:   this.sensors.windSpeed.measure(truth.windSpeed),
        smokeLevel:  this.sensors.smokeLevel.measure(truth.smokeLevel),
      },
    };
  }

  publish() {
    const payload = this.buildPayload();
    this.client.publish(this.topic, JSON.stringify(payload));
    return payload;
  }

  start() {
    // With staggerMs=0 every node publishes in phase, so they update in parallel
    const jitter = config.staggerMs > 0 ? Math.random() * config.staggerMs : 0;
    const kick = () => {
      this.publish();
      this.timer = setInterval(() => this.publish(), config.publishIntervalMs);
    };
    if (jitter > 0) setTimeout(kick, jitter);
    else kick();
  }
  stop() { clearInterval(this.timer); }
}

// Run a single node directly
if (require.main === module) {
  const client = mqtt.connect(config.brokerUrl, config.mqttOptions());
  client.on("connect", () => {
    console.log(`Connected to ${config.brokerUrl}`);
    const node = new SensorNode({
      nodeId: process.env.NODE_ID || "node_001",
      region: process.env.REGION || "region_01",
      location: { latitude: -37.8136, longitude: 144.9631 }, // Melbourne
      baseTemp: 22, tempAmp: 8, baseHumidity: 45, baseWind: 12,
      source: createDataSource(),
      client,
    });
    const tick = () => {
      const p = node.publish();
      console.log(node.topic, JSON.stringify(p.readings));
    };
    tick();
    setInterval(tick, config.publishIntervalMs);
  });
  client.on("error", (e) => console.error("MQTT error:", e.message));
}

module.exports = { SensorNode, Sensor };
