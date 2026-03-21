#!/bin/bash

# Start FastAPI backend
cd /app/backend
uvicorn main:app --host 0.0.0.0 --port 8000 &

# Start Next.js frontend
cd /app/frontend
npm run start
