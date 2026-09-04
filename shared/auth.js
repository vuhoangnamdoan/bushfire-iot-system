const crypto = require("crypto");

function isEnabled() {
  return String(process.env.AUTH_ENABLED || "false").toLowerCase() === "true";
}

// Constant-time compare that never throws on length mismatch.
function safeEqual(a, b) {
  const ab = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

// Pull a bearer token out of the Authorization header, or null.
function bearerToken(req) {
  const header = req.headers["authorization"] || "";
  const [scheme, value] = header.split(" ");
  if (!scheme || scheme.toLowerCase() !== "bearer" || !value) return null;
  return value.trim();
}

// Express middleware factory. Returns 401 on missing/invalid token when auth is
// enabled; passes straight through when AUTH_ENABLED is not "true".
function requireApiToken() {
  return function (req, res, next) {
    if (!isEnabled()) return next();

    const expected = process.env.API_TOKEN;
    if (!expected) {
      // Misconfiguration: auth is on but no token is set. Fail closed and make
      // the reason obvious in the logs rather than silently allowing traffic.
      console.error("[auth] AUTH_ENABLED=true but API_TOKEN is not set — rejecting request");
      return res.status(401).json({ error: "server auth misconfigured" });
    }

    const token = bearerToken(req);
    if (!token || !safeEqual(token, expected)) {
      return res.status(401).json({ error: "unauthorized" });
    }
    return next();
  };
}

// Build the Authorization header for outbound service-to-service calls. Returns
// {} when auth is disabled or no token is set, so callers work in both modes.
function authHeaders(extra = {}) {
  if (!isEnabled() || !process.env.API_TOKEN) return { ...extra };
  return { Authorization: `Bearer ${process.env.API_TOKEN}`, ...extra };
}

module.exports = { requireApiToken, authHeaders, isEnabled };
