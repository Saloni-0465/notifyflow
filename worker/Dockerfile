FROM node:22-alpine
WORKDIR /app
COPY worker/package.json ./package.json
RUN npm install --omit=dev
COPY worker/src ./src
CMD ["npm", "start"]
