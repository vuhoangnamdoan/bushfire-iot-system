const DROUGHT_FACTOR = clampNum(Number(process.env.DROUGHT_FACTOR || 9), 1, 10);

const SMOKE_ELEVATED = Number(process.env.SMOKE_ELEVATED_PPM || 150); // ppm
const SMOKE_CRITICAL = Number(process.env.SMOKE_CRITICAL_PPM || 300); // ppm

function clampNum(x, lo, hi) {
  if (Number.isNaN(x)) return lo;
  return Math.max(lo, Math.min(hi, x));
}

// Compute FFDI for a single reading.
function computeFFDI({ temperature, humidity, windSpeed }, droughtFactor = DROUGHT_FACTOR) {
  const T = temperature ?? 20;
  const RH = clampNum(humidity ?? 50, 0.1, 100); // avoid log/degenerate 0
  const V = windSpeed ?? 0;
  const DF = clampNum(droughtFactor, 1, 10);

  const ffdi =
    2 * Math.exp(-0.45 + 0.987 * Math.log(DF) - 0.0345 * RH + 0.0338 * T + 0.0234 * V);

  return Math.round(ffdi);
}

// Map an FFDI score then to the severity class
function ratingForFFDI(ffdi) {
  if (ffdi < 12) return { band: "low-moderate", severity: "low" };
  if (ffdi < 25) return { band: "high", severity: "moderate" };
  if (ffdi < 50) return { band: "very-high", severity: "moderate" };
  if (ffdi < 75) return { band: "severe", severity: "high" };
  if (ffdi < 100) return { band: "extreme", severity: "critical" };
  return { band: "catastrophic", severity: "critical" };
}

const SEVERITY_RANK = { low: 0, moderate: 1, high: 2, critical: 3 };

function maxSeverity(a, b) {
  return SEVERITY_RANK[a] >= SEVERITY_RANK[b] ? a : b;
}

// Evaluate a reading and decide whether it warrants an alert.
function evaluate(readings, opts = {}) {
  const droughtFactor = opts.droughtFactor ?? DROUGHT_FACTOR;
  const ffdiScore = computeFFDI(readings, droughtFactor);
  const rating = ratingForFFDI(ffdiScore);

  let severity = rating.severity;
  const reasons = [`FFDI ${ffdiScore} (${rating.band})`];

  // Smoke override - escalate on direct combustion evidence.
  const smoke = readings.smokeLevel ?? 0;
  if (smoke >= SMOKE_CRITICAL) {
    severity = maxSeverity(severity, "critical");
    reasons.push(`smoke ${smoke}ppm >= critical ${SMOKE_CRITICAL}`);
  } else if (smoke >= SMOKE_ELEVATED) {
    severity = maxSeverity(severity, "high");
    reasons.push(`smoke ${smoke}ppm >= elevated ${SMOKE_ELEVATED}`);
  }

  // Trigger an alert at "high" or above (severe FFDI or elevated smoke).
  const triggered = SEVERITY_RANK[severity] >= SEVERITY_RANK.high;

  return { ffdiScore, band: rating.band, severity, reasons, triggered };
}

module.exports = {
  computeFFDI,
  ratingForFFDI,
  evaluate,
  maxSeverity,
  SEVERITY_RANK,
  DROUGHT_FACTOR,
};
