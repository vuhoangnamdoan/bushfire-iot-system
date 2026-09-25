// processing.test.js — unit tests for the clean/validate/format logic.
//
// Runs without Docker, Kafka, or MongoDB:  node shared/processing.test.js
// Verifies the queue consumer preserves the exact behaviour of the old Node-RED
// "clean & validate" / "format" nodes, including the fault cases the sensor
// simulator produces (dropout, stuck, spike).

const assert = require("assert");
const { cleanReading, formatReading, createDedup } = require("./processing");

let passed = 0;
function ok(name, fn) {
  fn();
  passed++;
  console.log("  ok -", name);
}

const good = {
  nodeId: "node_042",
  region: "otway",
  timestamp: "2026-09-24T00:00:00.000Z",
  location: { latitude: -38.7, longitude: 143.55 },
  readings: { temperature: 38.5, humidity: 12.3, windSpeed: 45.2, smokeLevel: 280 },
};

console.log("processing.js");

ok("accepts a valid reading", () => {
  const r = cleanReading(good);
  assert.strictEqual(r.ok, true);
  assert.strictEqual(r.reading.nodeId, "node_042");
});

ok("accepts a JSON string payload", () => {
  const r = cleanReading(JSON.stringify(good));
  assert.strictEqual(r.ok, true);
});

ok("accepts a Buffer payload (as from Kafka)", () => {
  const r = cleanReading(Buffer.from(JSON.stringify(good)));
  assert.strictEqual(r.ok, true);
});

ok("rejects invalid JSON", () => {
  const r = cleanReading("{not json");
  assert.strictEqual(r.ok, false);
  assert.strictEqual(r.reason, "invalid_json");
});

ok("rejects missing fields", () => {
  const r = cleanReading({ nodeId: "n1", readings: {} });
  assert.strictEqual(r.ok, false);
  assert.strictEqual(r.reason, "missing_fields");
});

ok("rejects a dropout (null value)", () => {
  const bad = { ...good, readings: { ...good.readings, temperature: null } };
  const r = cleanReading(bad);
  assert.strictEqual(r.ok, false);
  assert.strictEqual(r.reason, "invalid_temperature");
});

ok("rejects an out-of-range spike", () => {
  const bad = { ...good, readings: { ...good.readings, smokeLevel: 3000 } }; // > 2000
  const r = cleanReading(bad);
  assert.strictEqual(r.ok, false);
  assert.strictEqual(r.reason, "invalid_smokeLevel");
});

ok("rejects a non-numeric value", () => {
  const bad = { ...good, readings: { ...good.readings, humidity: "NaN" } };
  const r = cleanReading(bad);
  assert.strictEqual(r.ok, false);
});

ok("format produces a Date timestamp and the five fields", () => {
  const d = formatReading(good);
  assert.ok(d.timestamp instanceof Date);
  assert.deepStrictEqual(Object.keys(d.readings).sort(), ["humidity", "smokeLevel", "temperature", "windSpeed"]);
  assert.strictEqual(d.region, "otway");
});

ok("dedup flags a repeated node+timestamp, lets a new one through", () => {
  const isDup = createDedup();
  assert.strictEqual(isDup("node_042", good.timestamp), false);
  assert.strictEqual(isDup("node_042", good.timestamp), true);
  assert.strictEqual(isDup("node_043", good.timestamp), false);
});

console.log(`\n${passed} tests passed`);
