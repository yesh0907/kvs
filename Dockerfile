# Common build stage
FROM node:14.14.0-alpine3.12 as common-build-stage

WORKDIR /app

EXPOSE 8080

CMD ["npm", "run", "start"]

COPY package.json /app

RUN npm install

COPY . /app

# Production build stage
FROM common-build-stage as production-build-stage

ENV NODE_ENV production
