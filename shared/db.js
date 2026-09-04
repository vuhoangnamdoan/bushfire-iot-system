const mongoose = require("mongoose");

// --- connection ------------------------------------------------------------
// connectDB retries a few times because in Docker the service container often
// starts before MongoDB has finished booting.
async function connectDB(uri, { retries = 10, delayMs = 2000 } = {}) {
  const url = uri || process.env.MONGO_URL || "mongodb://localhost:27017/bushfire";
  mongoose.set("strictQuery", true);

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await mongoose.connect(url);
      console.log(`[db] connected to ${url}`);
      return mongoose.connection;
    } catch (err) {
      console.error(`[db] connect attempt ${attempt}/${retries} failed: ${err.message}`);
      if (attempt === retries) throw err;
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
}

// --- shared sub-documents --------------------------------------------------
const locationSchema = new mongoose.Schema(
  {
    latitude: Number,
    longitude: Number,
  },
  { _id: false }
);

// --- readings collection ---------------------------------------------------
// Matches the sensor-node payload (nodeId, region, timestamp, location,
// readings{...}). Kept as a normal collection with a compound (region,
// timestamp) index, which is what the detection service and historical
// queries filter on.
const readingSchema = new mongoose.Schema(
  {
    nodeId: { type: String, required: true, index: true },
    region: { type: String, required: true, index: true },
    timestamp: { type: Date, required: true, index: true },
    location: locationSchema,
    readings: {
      temperature: Number, // Celsius
      humidity: Number,    // %
      windSpeed: Number,   // km/h
      smokeLevel: Number,  // ppm
    },
  },
  { timestamps: true } // adds createdAt/updatedAt (ingestion latency evidence)
);
readingSchema.index({ region: 1, timestamp: -1 });

// --- alerts collection -----------------------------------------------------
const alertSchema = new mongoose.Schema(
  {
    alertId: { type: String, required: true, unique: true },
    timestamp: { type: Date, required: true },
    region: { type: String, index: true },
    severity: { type: String }, // low | moderate | high | critical
    location: locationSchema,
    triggeringData: {
      temperature: Number,
      humidity: Number,
      windSpeed: Number,
      smokeLevel: Number,
      ffdiScore: Number, // McArthur Forest Fire Danger Index
    },
    triggeringNodes: [String],
    status: { type: String, default: "dispatched" }, // dispatched | acknowledged | resolved
    notifiedParties: [String],
  },
  { timestamps: true }
);
alertSchema.index({ region: 1, timestamp: -1 });

// `mongoose.models.X ||` guards against "OverwriteModelError" when the file is
// required more than once in the same process (e.g. during tests).
const Reading = mongoose.models.Reading || mongoose.model("Reading", readingSchema);
const Alert = mongoose.models.Alert || mongoose.model("Alert", alertSchema);

module.exports = { mongoose, connectDB, Reading, Alert };
