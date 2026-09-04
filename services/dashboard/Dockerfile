# Build context is the repository ROOT (see docker-compose build.context: .).
FROM node:20-alpine
WORKDIR /app

# 1) Reuse the detection service's pure scoring logic (rule.js). It has no
#    dependencies of its own, so copying the single file is enough.
COPY services/detection/rule.js services/detection/

# 2) This service.
COPY services/dashboard/package.json services/dashboard/package-lock.json* services/dashboard/
RUN cd services/dashboard && npm install --omit=dev
COPY services/dashboard/server.js services/dashboard/
COPY services/dashboard/public services/dashboard/public

WORKDIR /app/services/dashboard
EXPOSE 4004
CMD ["node", "server.js"]
