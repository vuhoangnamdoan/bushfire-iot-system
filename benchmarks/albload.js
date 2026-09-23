// albload.js — drive the ALB ingestion endpoint at a fixed rate.
// Usage: TARGET=http://<ALB-DNS> RATE=400 DURATION_SEC=180 node albload.js
const TARGET = process.env.TARGET || "http://localhost:4001";
const RATE = Number(process.env.RATE || 200);            // requests per second
const DURATION_SEC = Number(process.env.DURATION_SEC || 120);
const REGIONS = ["kinglake", "grampians", "dandenong", "otway", "macedon"];

let sent = 0, ok = 0, failed = 0;
const lat = [];

function oneRequest() {
  const region = REGIONS[Math.floor(Math.random() * REGIONS.length)];
  const body = {
    nodeId: "load_" + Math.floor(Math.random() * 100000),
    region,
    timestamp: new Date().toISOString(),
    location: { latitude: -37.5, longitude: 145.3 },
    readings: {
      temperature: 20 + Math.random() * 25,
      humidity: 10 + Math.random() * 50,
      windSpeed: Math.random() * 40,
      smokeLevel: Math.random() * 300,
    },
  };
  const t0 = Date.now();
  sent++;
  fetch(`${TARGET}/api/sensor-data`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  })
    .then((r) => { r.ok ? ok++ : failed++; lat.push(Date.now() - t0); })
    .catch(() => { failed++; });
}

function pct(a, p) {
  if (!a.length) return 0;
  const s = [...a].sort((x, y) => x - y);
  return s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))];
}

console.log(`Load: ${RATE} req/s for ${DURATION_SEC}s -> ${TARGET}`);
const tick = setInterval(() => { for (let i = 0; i < RATE; i++) oneRequest(); }, 1000);
setTimeout(() => {
  clearInterval(tick);
  setTimeout(() => {
    console.log(`sent=${sent} ok=${ok} failed=${failed} lossPct=${((failed / sent) * 100).toFixed(1)}`);
    console.log(`latency p50=${pct(lat,50)}ms p95=${pct(lat,95)}ms max=${pct(lat,100)}ms`);
  }, 3000);
}, DURATION_SEC * 1000);