// kafka.js — shared Kafka/Redpanda client helper for the bushfire services.
//
// Redpanda speaks the Kafka protocol, so we use the standard kafkajs client.
// One place builds the client and (optionally) creates the topic, so the
// bridge and every consumer share the same connection settings.

const { Kafka, logLevel } = require("kafkajs");

// Comma-separated broker list, e.g. "redpanda:9092". Inside Docker the services
// reach Redpanda by its compose service name; from the host use localhost:19092.
const BROKERS = (process.env.KAFKA_BROKERS || "redpanda:9092")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const TOPIC = process.env.KAFKA_TOPIC || "sensor-readings";

// Number of partitions decides the maximum consumer parallelism: a consumer
// group can run at most one consumer per partition. 12 lets us scale the
// ingestion tier out to 12 instances for the scaling experiment.
const PARTITIONS = Number(process.env.KAFKA_PARTITIONS || 12);

function buildKafka(clientId) {
  return new Kafka({
    clientId: clientId || "bushfire",
    brokers: BROKERS,
    logLevel: logLevel.NOTHING, // keep the throughput logs clean
    retry: { initialRetryTime: 300, retries: 10 },
  });
}

// Create the topic if it does not exist. Safe to call on every startup.
async function ensureTopic(kafka, { topic = TOPIC, partitions = PARTITIONS } = {}) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = await admin.listTopics();
    if (!existing.includes(topic)) {
      await admin.createTopics({
        topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
        waitForLeaders: true,
      });
      console.log(`[kafka] created topic "${topic}" with ${partitions} partitions`);
    } else {
      console.log(`[kafka] topic "${topic}" already exists`);
    }
  } finally {
    await admin.disconnect();
  }
}

module.exports = { buildKafka, ensureTopic, BROKERS, TOPIC, PARTITIONS };
