# Build context is the repository ROOT (see docker-compose build.context: .).
FROM node:20-alpine
WORKDIR /app

# 1) Shared DB module.
COPY shared/package.json shared/
RUN cd shared && npm install --omit=dev
COPY shared/db.js shared/auth.js shared/

# 2) This service.
COPY services/alert/package.json services/alert/package-lock.json* services/alert/
RUN cd services/alert && npm install --omit=dev
COPY services/alert/server.js services/alert/

WORKDIR /app/services/alert
EXPOSE 4003
CMD ["node", "server.js"]
