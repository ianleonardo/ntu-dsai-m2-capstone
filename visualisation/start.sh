#!/bin/bash

# Start FastAPI backend (uv-managed env from uv.lock)
cd /app/backend
uv run uvicorn main:app --host 0.0.0.0 --port 8000 &

# Start Next.js frontend
cd /app/frontend
npm run start
