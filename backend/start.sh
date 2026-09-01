#!/bin/bash

# Start the LiveKit agent worker in the background
python agent_worker.py start &

# Start the FastAPI server using Gunicorn for production readiness
PORT=${PORT:-5000}
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:$PORT
