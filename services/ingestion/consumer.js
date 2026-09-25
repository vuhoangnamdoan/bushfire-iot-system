// ingestion/consumer.js — Kafka consumer that writes readings to MongoDB.
//
// This is the scalable replacement for the old path (Node-RED -> HTTP POST per
// message). It consumes the "sensor-readings" topic in the "ingestion" consumer
// group, cleans/validates each reading (same rules as the old Node-RED flow),
// and writes them to Mongo in batches with insertMany. Two things make this
// scale where Node-RED did not:
//   1. Batched writes: one insertMany per Kafka batch instead of one write per
//      message, which cuts database round-trips dramatically.
//   2. Horizontal parallelism: run N instances in the same group and Redpanda
//      spreads the partitions across them (up to KAFKA_PARTITIONS instances).

const { buildKafka, ensureTopic, TOPIC } = require("../../shared/kafka");
const { Reading } = require("../../shared/db");
const { cleanReading, formatReading, createDedup } = require("../../shared/processing");

const GROUP = process.env.KAFKA_GROUP || "ingestion";

async function startConsumer() {
  const kafka = buildKafka(`ingestion-${process.pid}`);
  await ensureTopic(kafka); // no-op if the bridge already created it

  const consumer = kafka.consumer({
    groupId: GROUP,
    // Give a busy consumer room before the broker considers it dead.
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
  });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });
  console.log(`[ingestion] consuming "${TOPIC}" in group "${GROUP}"`);

  const isDuplicate = createDedup();
  let stored = 0;
  let dropped = 0;
  let lastReport = 0;
  setInterval(() => {
    const rate = stored - lastReport;
    lastReport = stored;
    console.log(`[metrics] ingest stored/sec=${rate} total=${stored} dropped=${dropped}`);
  }, 1000);

  await consumer.run({
    // eachBatch lets us turn a whole Kafka batch into a single insertMany.
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      const docs = [];
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) break;
        const clean = cleanReading(message.value);
        if (!clean.ok) {
          dropped++;
          resolveOffset(message.offset);
          continue;
        }
        const doc = formatReading(clean.reading);
        if (isDuplicate(doc.nodeId, doc.timestamp.toISOString())) {
          dropped++;
          resolveOffset(message.offset);
          continue;
        }
        docs.push(doc);
        resolveOffset(message.offset);
      }

      if (docs.length) {
        try {
          // ordered:false so one bad doc can't abort the whole batch.
          await Reading.insertMany(docs, { ordered: false });
          stored += docs.length;
        } catch (e) {
          // insertMany with ordered:false still writes the good docs; count them.
          const n = e.insertedDocs ? e.insertedDocs.length : 0;
          stored += n;
          console.error(`[ingestion] batch insert partial/failed: ${e.message} (wrote ${n}/${docs.length})`);
        }
      }
      await heartbeat();
    },
  });

  const shutdown = async () => {
    console.log(`\n[ingestion] consumer stopping. Total stored: ${stored}, dropped: ${dropped}`);
    try { await consumer.disconnect(); } catch (_) {}
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  return consumer;
}

module.exports = { startConsumer };
