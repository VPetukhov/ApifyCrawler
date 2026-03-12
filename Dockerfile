FROM apify/actor-node-playwright-chrome:24-1.58.2

COPY --chown=myuser:myuser package*.json ./
RUN npm install --include=dev --no-audit

COPY --chown=myuser:myuser . ./
RUN npm run build

CMD npm run start:prod
