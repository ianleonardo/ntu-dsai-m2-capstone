# Multi-stage Dockerfile for Insider Alpha Dashboard

# --- Frontend Build Stage ---
FROM node:20-slim AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ ./
# Set API URL for build time if needed
ENV NEXT_PUBLIC_API_URL=/api
RUN npm run build

# --- Backend Stage & Final Image ---
FROM python:3.11-slim
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y nodejs npm && rm -rf /var/lib/apt/lists/*

# Copy Backend
COPY backend/requirements.txt ./backend/
RUN pip install --no-cache-dir -r backend/requirements.txt
COPY backend/ ./backend/

# Copy Frontend Build and Source
COPY --from=frontend-builder /app/frontend/.next ./frontend/.next
COPY --from=frontend-builder /app/frontend/public ./frontend/public
COPY --from=frontend-builder /app/frontend/package*.json ./frontend/
COPY frontend/node_modules ./frontend/node_modules

# Copy startup script
COPY start.sh ./
RUN chmod +x start.sh

EXPOSE 8000 3000

CMD ["./start.sh"]
