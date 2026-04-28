#!/bin/bash

# Real-time streamer runner script
# Usage: ./run_streamer.sh [interval_in_seconds]

echo "🚀 Starting Real-time Market Data Streamer"
echo "=========================================="

# Check if interval argument is provided
if [ $# -eq 1 ]; then
    INTERVAL=$1
    echo "Using command line interval: ${INTERVAL} seconds"
    python3 streamer.py $INTERVAL
else
    echo "Using configured interval from .env file"
    python3 streamer.py
fi
