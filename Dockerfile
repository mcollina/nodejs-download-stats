FROM node:22-alpine AS builder

ENV APP_HOME=/home/app/node/
WORKDIR $APP_HOME

COPY package.json package-lock.json .

RUN npm ci 

COPY . .

FROM node:18-alpine

ENV APP_HOME=/home/app/node/
WORKDIR $APP_HOME

COPY package.json package-lock.json ./
RUN npm ci --only=production

COPY --from=builder $APP_HOME .

ENV PLT_SERVER_HOSTNAME=0.0.0.0
ENV PORT=3042
ENV PLT_SERVER_LOGGER_LEVEL=info

EXPOSE 3042

CMD ["node", "node_modules/.bin/wattpm", "start"]
