FROM node:24-alpine AS build

WORKDIR /app

COPY package*.json ./

RUN npm ci

COPY . .

RUN npm run build

FROM nginx:alpine

COPY --from=build /app/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=10 CMD ["wget", "-q", "--spider", "http://127.0.0.1/"]

CMD ["nginx", "-g", "daemon off;"]
