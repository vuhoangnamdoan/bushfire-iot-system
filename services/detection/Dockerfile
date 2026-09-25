# Build context is the repository ROOT (see docker-compose build.context: .).
FROM node:20-alpine
WORKDIR /app

# 1) Shared DB module.
COPY shared/package.json shared/
RUN cd shared && npm install --omit=dev
COPY shared/db.js shared/auth.js shared/kafka.js shared/processing.js shared/

# 2) This service (rule.js is the pure scoring logic used by server.js).
COPY services/detection/package.json services/detection/package-lock.json* services/detection/
RUN cd services/detection && npm install --omit=dev
COPY services/detection/server.js services/detection/consumer.js services/detection/rule.js services/detection/

WORKDIR /app/services/detection
EXPOSE 4002
CMD ["node", "server.js"]
