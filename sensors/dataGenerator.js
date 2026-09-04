const config = require("./config");

// math helper
function randn() {
  // Standard normal (Box-Muller) for realistic Gaussian noise.
  let u = 0, v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}
function clamp(x, lo, hi) { return Math.max(lo, Math.min(hi, x)); }

// wall-clock time -> simulated hour of day (0..24)
function simHourOfDay(nowMs) {
  const hours = ((nowMs * config.timeScale) / 3600000) % 24;
  return hours;
}

// Synthetic source: physically-motivated model.
//   - temperature: daily (diurnal) cycle, peak ~15:00, low ~03:00
//   - humidity:    inversely related to temperature
//   - wind:        mean-reverting random walk (Ornstein-Uhlenbeck) + gusts
//   - smoke:       low background, rises sharply during a fire event
//   - fire:        per-node state machine (none -> igniting -> growing -> active -> declining) that pushes the other features
class SyntheticSource {
  constructor() {
    this.state = new Map(); // per-node evolving state (wind, fire, drought)
  }

  _stateFor(node) {
    if (!this.state.has(node.nodeId)) {
      this.state.set(node.nodeId, {
        wind: node.baseWind,
        drought: 8 + Math.random() * 2,        // McArthur drought factor 0..10
        fire: { phase: "none", intensity: 0, startSim: 0 },
        lastMs: Date.now(),
      });
    }
    return this.state.get(node.nodeId);
  }

  // Force a node into an active fire immediately (burst/event-intensity test).
  // Sets a high intensity and back-dates the fire start so the intensity target
  ignite(node, nowMs = Date.now(), intensity = 0.9) {
    const s = this._stateFor(node);
    const nowSim = nowMs * config.timeScale;
    s.fire = {
      phase: "active",
      intensity,
      startSim: nowSim - 25 * 60000, // 25 simulated minutes in -> target intensity ~1
    };
  }

  sample(node, nowMs) {
    const s = this._stateFor(node);
    const dt = Math.max(0.001, (nowMs - s.lastMs) / 1000); // seconds elapsed
    s.lastMs = nowMs;

    // 1) Diurnal temperature.
    const h = simHourOfDay(nowMs);
    const diurnal = Math.sin(((h - 9) / 24) * 2 * Math.PI); // min ~03h, peak ~15h
    let temperature = node.baseTemp + node.tempAmp * diurnal + randn() * 0.3;

    // 2) Humidity, inversely related to temperature.
    let humidity = clamp(
      node.baseHumidity - 1.4 * (temperature - node.baseTemp) + randn() * 2,
      3, 100
    );

    // 3) Wind: mean-reverting random walk with occasional gusts.
    const theta = 0.15, sigma = 3.0;
    s.wind += theta * (node.baseWind - s.wind) * dt + sigma * Math.sqrt(dt) * randn();
    if (Math.random() < 0.02) s.wind += 10 + Math.random() * 20; // gust
    s.wind = clamp(s.wind, 0, 120);
    let windSpeed = s.wind;

    // 4) Smoke background.
    let smokeLevel = 8 + randn() * 2;

    // fire event state machine
    const fire = s.fire;
    const nowSim = nowMs * config.timeScale;
    if (fire.phase === "none") {
      if (Math.random() < config.ignitionChance) {
        fire.phase = "igniting";
        fire.startSim = nowSim;
        fire.intensity = 0;
      }
    } else {
      const mins = (nowSim - fire.startSim) / 60000; // simulated minutes elapsed
      if (fire.phase === "igniting"  && mins > 2)  fire.phase = "growing";
      if (fire.phase === "growing"   && mins > 15) fire.phase = "active";
      if (fire.phase === "active"    && mins > 60) fire.phase = "declining";
      if (fire.phase === "declining" && mins > 90) { fire.phase = "none"; fire.intensity = 0; }

      const target = fire.phase === "declining" ? 0 : Math.min(1, mins / 20);
      fire.intensity += (target - fire.intensity) * clamp(dt * 0.05, 0, 1);
    }

    // Fire raises temperature and smoke, drops humidity, lifts wind a little.
    if (fire.intensity > 0) {
      temperature += 15 * fire.intensity + randn() * 1.5;
      humidity = clamp(humidity - 30 * fire.intensity, 2, 100);
      smokeLevel += 500 * fire.intensity + randn() * 20;
      s.wind = clamp(s.wind + 5 * fire.intensity, 0, 120);
      windSpeed = s.wind;
    }

    return {
      temperature: Number(temperature.toFixed(1)),
      humidity: Number(humidity.toFixed(1)),
      windSpeed: Number(windSpeed.toFixed(1)),
      smokeLevel: Number(Math.max(0, smokeLevel).toFixed(0)),
      _firePhase: fire.phase, // for logging/testing only, not published as-is
    };
  }
}

// Future real-data source. Must expose the same sample(node, nowMs) shape.
class RealSource {
  constructor() {
    throw new Error(
      "RealSource not implemented yet. Plug in a BOM historical CSV or a " +
      "weather API here and return { temperature, humidity, windSpeed, smokeLevel }."
    );
  }
}

function createDataSource() {
  switch (config.dataSource) {
    case "real": return new RealSource();
    case "synthetic":
    default: return new SyntheticSource();
  }
}

module.exports = { createDataSource, SyntheticSource, RealSource };
