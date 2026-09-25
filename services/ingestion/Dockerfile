# Build context is the repository ROOT (see docker-compose build.context: .)
FROM node:20-alpine
WORKDIR /app

# 1) Shared DB module (connection + Mongoose models) used by all services.
COPY shared/package.json shared/
RUN cd shared && npm install --omit=dev
COPY shared/db.js shared/auth.js shared/kafka.js shared/processing.js shared/

# 2) This service (install deps first for better layer caching).
COPY services/ingestion/package.json services/ingestion/package-lock.json* services/ingestion/
RUN cd services/ingestion && npm install --omit=dev
COPY services/ingestion/server.js services/ingestion/consumer.js services/ingestion/

WORKDIR /app/services/ingestion
EXPOSE 4001
CMD ["node", "server.js"]
