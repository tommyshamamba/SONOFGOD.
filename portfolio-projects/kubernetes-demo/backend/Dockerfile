FROM node:24-alpine

WORKDIR /app

COPY package*.json ./

RUN npm ci --omit=dev

COPY --chown=node:node . .

ENV HOST=0.0.0.0 PORT=3000
RUN mkdir -p /data && chown node:node /data
USER node

EXPOSE 3000

CMD ["node", "server.js"]
