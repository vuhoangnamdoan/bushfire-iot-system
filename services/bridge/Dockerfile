# Build context is the repository ROOT (see docker-compose build.context: .)
FROM node:20-alpine
WORKDIR /app

# Shared Kafka helper used by the bridge and consumers.
COPY shared/package.json shared/
RUN cd shared && npm install --omit=dev
COPY shared/kafka.js shared/

# This service.
COPY services/bridge/package.json services/bridge/package-lock.json* services/bridge/
RUN cd services/bridge && npm install --omit=dev
COPY services/bridge/server.js services/bridge/

WORKDIR /app/services/bridge
CMD ["node", "server.js"]
