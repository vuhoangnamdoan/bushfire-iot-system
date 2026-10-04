# Bushfire Detection Monitoring System

A small IoT stack that simulates a fleet of bushfire sensor nodes, streams their readings over MQTT, cleans and routes them through Node-RED, and runs three
cloud microservices (ingestion, detection, alert) backed by MongoDB, with a live dashboard.

```
sensors ──MQTT──▶ Mosquitto ──▶ Node-RED ──HTTP──▶ ingestion ──▶ MongoDB
                     │                    └─HTTP──▶ detection ──▶ alert ──▶ (IFTTT/log)
                     └────────────────────────────▶ dashboard (live map)
```
<img width="1072" height="301" alt="Screenshot 2026-09-04 at 16 57 16" src="https://github.com/user-attachments/assets/8176efb8-ae64-4506-aeb3-1df47f37401e" />


Run the whole stack with Docker Compose:

```bash
docker compose up --build
```

---

## HD (6.4HD): Redpanda queue pipeline

The 6.3D benchmarks showed the single-threaded **Node-RED** processing tier is
the scaling bottleneck (0% loss up to ~200 msg/s, then 85% loss at 5000 nodes,
99% beyond, while ingestion and MongoDB stay idle). The HD work applies a
state-of-the-art fix: put a durable, Kafka-compatible **Redpanda** queue between
the broker and the services, so ingestion is buffered and consumed in parallel
instead of funnelling through one process.

<img width="1011" height="260" alt="ab" src="https://github.com/user-attachments/assets/eaf3c413-37b0-4181-8852-f281338679df" />

What changed:
- **`services/bridge`** — thin MQTT→Kafka bridge (keys each message by nodeId, batches to Redpanda). Does almost no per-message work, so it is not a bottleneck.
- **`services/ingestion/consumer.js`** — consumes the `sensor-readings` topic in the `ingestion` group and writes with batched `insertMany`. Scale it out with `--scale ingestion=N` (up to `KAFKA_PARTITIONS`).
- **`services/detection/consumer.js`** — consumes the same topic in its own group (pub/sub fan-out), applies the smoke pre-filter, then the existing FFDI rule.
- **`shared/processing.js`** — the clean/validate/format logic, ported verbatim from the Node-RED function nodes so the two pipelines do equivalent work.
- Node-RED moved behind the `legacy` profile; Redpanda + bridge are under the `queue` profile, so the two write paths never run together.

### Run the two modes

```bash
# BEFORE (Node-RED path) — reproduces the 6.3D baseline
COMPOSE_PROFILES=legacy docker compose up --build

# AFTER (Redpanda queue path)
COMPOSE_PROFILES=queue KAFKA_ENABLED=true docker compose up --build
```

### Reproduce the experiment

```bash
# 1. bring up the queue stack
COMPOSE_PROFILES=queue KAFKA_ENABLED=true docker compose up -d --build

# 2. run the ramp (same steps as the 6.3D plan1 run)
cd benchmarks
STEPS=300,500,1000,5000,10000 HOLD_SEC=300 INTERVAL_MS=5000 node plan1_queue_throughput.js

# 3. draw before/after charts + table (needs plan1_results.json from 6.3D)
python3 plot_compare.py     # -> out/compare_loss.png, compare_latency.png, compare_table.md

# 4. (optional) show horizontal scaling: rerun step 2 with more consumers
COMPOSE_PROFILES=queue KAFKA_ENABLED=true \
  docker compose -f docker-compose.yaml -f docker-compose.scale.yaml \
  up -d --build --scale ingestion=3
```

Local logic tests (no Docker needed):

```bash
node shared/processing.test.js
```

---

## Security

| Control | What it protects | Flag | Off (default) | On |
|---|---|---|---|---|
| 1. MQTT authentication | Broker access | `MOSQUITTO_CONF` | anonymous (`mosquitto.dev.conf`) | username/password (`mosquitto.conf`) |
| 2. MQTT over TLS (MQTTS) | Sensor→broker transport | `TLS_ENABLED` | plain `mqtt://…:1883` | `mqtts://…:8883` + CA verify |
| 3. REST API token auth | Service write endpoints | `AUTH_ENABLED` | endpoints open | `Authorization: Bearer <token>` required |
| 4. Secrets & exposure hygiene | Credentials, network | — | env vars + `.env`; internal-only network on AWS | — |

### 1. MQTT authentication (broker)

The secured broker config [`mqtt/mosquitto.conf`](mqtt/mosquitto.conf) sets `allow_anonymous false` and a `password_file`, so every client must present a
username and password. The open config [`mqtt/mosquitto.dev.conf`](mqtt/mosquitto.dev.conf) keeps anonymous access for tests. `MOSQUITTO_CONF` selects which one Compose loads.

Create the password file (uses the eclipse-mosquitto image, so no local install
needed):

```bash
MQTT_USERNAME=bushfire MQTT_PASSWORD='your-strong-pass' ./security/gen-passwd.sh
```


```bash
mosquitto_passwd -c -b mqtt/passwd "$MQTT_USERNAME" "$MQTT_PASSWORD"
```

Clients send `MQTT_USERNAME` / `MQTT_PASSWORD` from the environment:
- the sensor scripts ([`sensors/config.js`](sensors/config.js) → `fleet.js`/`sensorNode.js`),
- the dashboard ([`services/dashboard/server.js`](services/dashboard/server.js)), and 
- Node-RED (the broker node reads `${MQTT_USERNAME}`/`${MQTT_PASSWORD}`).

### 2. MQTT over TLS (MQTTS)

[`security/gen-certs.sh`](security/gen-certs.sh) uses OpenSSL to create a self-signed **root CA** and a **broker server certificate** (with Subject Alternative Names for `mosquitto`, `localhost`, and `127.0.0.1`, so verification works both inside the Compose network and from the host):

```bash
./security/gen-certs.sh    # -> security/ca.crt, ca.key, broker.crt, broker.key
```

The secured broker adds a **TLS listener on 8883** using `cafile`/`certfile`/
`keyfile`. The plain **1883 listener stays available** (authenticated) so local non-TLS runs and tests keep working. When `TLS_ENABLED=true`, clients connect with `mqtts://…:8883` and verify the broker against `ca.crt` (`MQTT_CA_FILE`) — host scripts default the CA path to `security/ca.crt`, the containers use the mounted `/security/ca.crt`.

### 3. REST API token auth (microservices)

[`shared/auth.js`](shared/auth.js) is a tiny Express middleware that checks `Authorization: Bearer <token>` against the `API_TOKEN` environment variable and returns **401** on a missing or invalid token. It's a no-op when `AUTH_ENABLED` is not `true`, so tests run unprotected.

It's applied to the **write** endpoints only — `/health` stays open for the load balancer:

| Service | Protected endpoint |
|---|---|
| ingestion | `POST /api/sensor-data` |
| detection | `POST /api/detect` |
| alert | `POST /api/alert` |

Callers send the token: Node-RED's HTTP request nodes add the header (in the `format` function) when `AUTH_ENABLED=true`, and the detection→alert Axios call uses `authHeaders()` from the same shared module.

---

## Running with security on vs off

**Off (default — for scaling/throughput tests).** Nothing to set up:

```bash
docker compose up --build
# sensors (host):
cd sensors && node fleet.js
```

**On (full security layer):**

```bash
cp .env.example .env            # then edit values

# one-time key material
MQTT_USERNAME=bushfire MQTT_PASSWORD='your-pass' ./security/gen-passwd.sh
./security/gen-certs.sh

# in .env, set:
#   AUTH_ENABLED=true
#   TLS_ENABLED=true
#   MOSQUITTO_CONF=mosquitto.conf
#   MQTT_URL=mqtts://mosquitto:8883
#   API_TOKEN=<openssl rand -hex 32>
#   MQTT_USERNAME=bushfire
#   MQTT_PASSWORD=your-pass

docker compose up --build

# sensors (host) — export the same values, then:
cd sensors
TLS_ENABLED=true MQTT_USERNAME=bushfire MQTT_PASSWORD='your-pass' \
  MQTT_URL=mqtts://localhost:8883 node fleet.js
```

With auth on, an unauthenticated write is rejected:

```bash
curl -s -X POST localhost:4001/api/sensor-data -d '{}' -H 'content-type: application/json'
# -> 401 {"error":"unauthorized"}

curl -s -X POST localhost:4001/api/sensor-data \
  -H "authorization: Bearer $API_TOKEN" -H 'content-type: application/json' \
  -d '{"nodeId":"n1","region":"otway","readings":{"temperature":30,"humidity":20,"windSpeed":10,"smokeLevel":50}}'
# -> 201 {"ok":true,...}
```
