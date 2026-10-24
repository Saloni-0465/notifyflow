FROM node:22-alpine
WORKDIR /app
COPY api/package.json ./package.json
RUN npm install --omit=dev
COPY api/src ./src
EXPOSE 3000
CMD ["npm", "start"]
