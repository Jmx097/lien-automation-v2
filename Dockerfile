# Use the official Playwright image — has Chromium + all system deps pre-installed
FROM node:20-alpine
# Using Alpine instead of Playwright image (~150MB vs ~1.5GB)
# Browser runs remotely via Bright Data Scraping Browser in production

WORKDIR /app

# Copy package files first (layer caching — only re-runs npm ci if these change)
COPY package*.json ./

# Install dependencies
RUN npm ci

# OCR runtime dependencies for image-based CA SOS PDFs
RUN apk add --no-cache \
  poppler-utils \
  tesseract-ocr \
  tesseract-ocr-data-eng

# Copy the rest of the source
COPY . .

# Increase Node heap for the TypeScript compile step
ENV NODE_OPTIONS="--max-old-space-size=4096"

# Compile TypeScript → dist/
RUN npm run build

# Runtime env
ENV NODE_ENV=production
ENV PORT=8080

# Expose the port Cloud Run expects
EXPOSE 8080

# Run the compiled server
CMD ["node", "dist/src/server.js"]
