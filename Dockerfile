# Use the official Playwright image — has Chromium + all system deps pre-installed
FROM node:20-alpine
# Using Alpine instead of Playwright image (~150MB vs ~1.5GB)
# Browser runs remotely via Bright Data Scraping Browser in production

WORKDIR /app

# Copy package files first (layer caching — only re-runs npm ci if these change)
COPY package*.json ./

# Install dependencies
RUN npm ci

# Copy the rest of the source
COPY . .

# Compile TypeScript → dist/
RUN npm run build

# Runtime env
ENV NODE_ENV=production
ENV PORT=8080

# Expose the port Cloud Run expects
EXPOSE 8080

# Run the compiled server
CMD ["node", "dist/server.js"]
