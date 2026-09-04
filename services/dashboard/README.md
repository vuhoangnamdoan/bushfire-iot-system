# Dashboard Service (port 4004)

A live view over the sensor fleet. It observes MQTT traffic, scores each reading with the same FFDI logic the detection service uses, and streams the
result to the browser.

## What it does

- Subscribes to the MQTT broker (`MQTT_URL`, default `mqtt://mosquitto:1883`) on `+/+/readings`.
- Enriches each reading with `ffdiScore` + `severity` via `services/detection/rule.js`.
- Keeps the latest reading per node in memory and pushes every enriched reading to browsers over Socket.IO.

### HTTP endpoints

| Method | Path                       | Purpose                                              |
|--------|----------------------------|------------------------------------------------------|
| GET    | `/health`                  | `{ status: "ok" }`                                   |
| GET    | `/api/nodes`               | Latest enriched reading per node (first paint)       |
| GET    | `/api/history?nodeId=...`  | Proxies `ingestion /api/readings?nodeId=...&limit=100` |
| —      | `/`                        | Serves the single-file frontend from `public/`       |

## Running it

The service is part of the stack:

```bash
docker compose up -d --build
```

Then open:

```
http://localhost:4004
```

Make sure some sensor nodes are publishing (see `sensors/`) so readings flow in.

## Configuration (environment)

| Variable         | Default                    | Meaning                                  |
|------------------|----------------------------|------------------------------------------|
| `PORT`           | `4004`                     | HTTP/WebSocket port                      |
| `MQTT_URL`       | `mqtt://mosquitto:1883`    | Broker to subscribe to                   |
| `INGESTION_URL`  | `http://ingestion:4001`    | Ingestion service used by `/api/history` |
| `MQTT_TOPIC`     | `+/+/readings`             | Subscription topic filter                |
| `DROUGHT_FACTOR` | `9`                        | Passed through to the FFDI scoring        |
