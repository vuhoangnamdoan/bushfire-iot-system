const mqtt = require("mqtt");
const config = require("./config");
const { createDataSource } = require("./dataGenerator");
const { SensorNode } = require("./sensorNode");

// Ten real forests across Victoria, Australia.
const forestProfiles = [
  { name: "otway",         lat: -38.70, lon: 143.55, spread: 0.15, baseTemp: 18, baseHumidity: 62, baseWind: 14 }, // Great Otway NP
  { name: "grampians",     lat: -37.20, lon: 142.45, spread: 0.15, baseTemp: 23, baseHumidity: 40, baseWind: 12 }, // Grampians (Gariwerd) NP
  { name: "alpine",        lat: -37.00, lon: 147.10, spread: 0.25, baseTemp: 14, baseHumidity: 55, baseWind: 16 }, // Alpine NP
  { name: "wilsons_prom",  lat: -39.00, lon: 146.35, spread: 0.12, baseTemp: 19, baseHumidity: 65, baseWind: 18 }, // Wilsons Promontory NP
  { name: "kinglake",      lat: -37.52, lon: 145.35, spread: 0.08, baseTemp: 21, baseHumidity: 45, baseWind: 11 }, // Kinglake NP (Black Saturday)
  { name: "yarra_ranges",  lat: -37.65, lon: 145.75, spread: 0.15, baseTemp: 20, baseHumidity: 58, baseWind: 10 }, // Yarra Ranges NP
  { name: "mt_buffalo",    lat: -36.73, lon: 146.80, spread: 0.08, baseTemp: 15, baseHumidity: 50, baseWind: 13 }, // Mount Buffalo NP
  { name: "errinundra",    lat: -37.30, lon: 148.85, spread: 0.08, baseTemp: 17, baseHumidity: 60, baseWind: 12 }, // Errinundra NP
  { name: "croajingolong", lat: -37.55, lon: 149.30, spread: 0.15, baseTemp: 20, baseHumidity: 63, baseWind: 15 }, // Croajingolong NP
  { name: "dandenong",     lat: -37.83, lon: 145.35, spread: 0.06, baseTemp: 20, baseHumidity: 55, baseWind: 9  }, // Dandenong Ranges NP
];

const FORESTS = Math.min(
  Number(process.env.FORESTS || process.env.REGIONS || forestProfiles.length),
  forestProfiles.length
);
const activeForests = forestProfiles.slice(0, FORESTS);

// Node-count model
const NODES_PER_FOREST = Number(process.env.NODES_PER_FOREST || 200);
const TOTAL_OVERRIDE = process.env.NODES ? Number(process.env.NODES) : null;

// forestIndex for each node, in creation order.
const assignment = [];
if (TOTAL_OVERRIDE != null) {
  for (let i = 0; i < TOTAL_OVERRIDE; i++) assignment.push(i % activeForests.length);
} else {
  for (let f = 0; f < activeForests.length; f++)
    for (let n = 0; n < NODES_PER_FOREST; n++) assignment.push(f);
}
const TOTAL = assignment.length;
const idPad = Math.max(3, String(TOTAL).length);

// Optional global override for the per-forest scatter radius (degrees).
const SPREAD_OVERRIDE = process.env.FOREST_SPREAD_DEG
  ? Number(process.env.FOREST_SPREAD_DEG)
  : null;

// Event-intensity burst test
const FORCE_FIRE = process.env.FORCE_FIRE === "1" || process.env.FORCE_FIRE === "true";
const BURST_AT_SEC = Number(process.env.BURST_AT_SEC || 3);
const BURST_FRACTION = Math.min(1, Math.max(0, Number(process.env.BURST_FRACTION || 1)));

const client = mqtt.connect(config.brokerUrl, config.mqttOptions());
const source = createDataSource();
let published = 0;

client.on("connect", () => {
  console.log(`Fleet connected to ${config.brokerUrl}`);
  if (TOTAL_OVERRIDE != null) {
    console.log(`Starting ${TOTAL} nodes round-robin across ${activeForests.length} forest(s), ` +
                `publishing every ${config.publishIntervalMs / 1000}s`);
  } else {
    console.log(`Starting ${activeForests.length} forest(s) x ${NODES_PER_FOREST} nodes = ${TOTAL}, ` +
                `publishing every ${config.publishIntervalMs / 1000}s`);
  }
  console.log(`Forests: ${activeForests.map((f) => f.name).join(", ")}`);

  const nodes = [];
  for (let i = 0; i < TOTAL; i++) {
    const f = activeForests[assignment[i]];
    const spread = SPREAD_OVERRIDE != null ? SPREAD_OVERRIDE : f.spread;
    const node = new SensorNode({
      nodeId: `node_${String(i + 1).padStart(idPad, "0")}`,
      region: f.name,
      location: {
        // Scatter each node uniformly within +/- `spread` degrees of the centre.
        latitude: f.lat + (Math.random() - 0.5) * 2 * spread,
        longitude: f.lon + (Math.random() - 0.5) * 2 * spread,
      },
      baseTemp: f.baseTemp, tempAmp: 8,
      baseHumidity: f.baseHumidity, baseWind: f.baseWind,
      source, client,
    });
    const origPublish = node.publish.bind(node);
    node.publish = () => { published++; return origPublish(); };
    node.start();
    nodes.push(node);
  }

  // Burst mode: ignite a fraction of nodes simultaneously after a short delay.
  if (FORCE_FIRE) {
    setTimeout(() => {
      const count = Math.ceil(nodes.length * BURST_FRACTION);
      const when = Date.now();
      for (let i = 0; i < count; i++) source.ignite(nodes[i], when);
      const regions = [...new Set(nodes.slice(0, count).map((n) => n.region))];
      console.log(`FIRE_IGNITED ${count} across ${regions.length} region(s) at ${new Date(when).toISOString()}`);
    }, BURST_AT_SEC * 1000);
  }

  // Throughput report once per second.
  let lastCount = 0;
  setInterval(() => {
    const rate = published - lastCount;
    lastCount = published;
    console.log(`msgs/sec: ${rate}   total: ${published}`);
  }, 1000);
});

client.on("error", (e) => console.error("Fleet MQTT error:", e.message));
process.on("SIGINT", () => { console.log(`\nStopped. Total published: ${published}`); process.exit(0); });
