#!/bin/bash

# Market Hours Parallel Update Loop
# Runs parallel_daily_update.py every 5 minutes during market hours (9:15 AM - 3:30 PM)
# Usage: ./run_market_hours_loop.sh [num_workers]

set -e

# Configuration
NUM_WORKERS=${1:-4}  # Default to 4 workers if not specified
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTERVAL_MINUTES=5
START_TIME="03:45"
END_TIME="10:00"

echo "=========================================="
echo "Market Hours Parallel Update Loop"
echo "=========================================="
echo "Workers: $NUM_WORKERS"
echo "Interval: $INTERVAL_MINUTES minutes"
echo "Market Hours: $START_TIME - $END_TIME"
echo "Script Directory: $SCRIPT_DIR"
echo "Started at: $(date)"
echo "=========================================="

# Change to script directory
cd "$SCRIPT_DIR"

# Function to check if current time is within market hours
is_market_hours() {
    local current_time=$(date +%H:%M)
    local current_seconds=$(date +%s -d "$current_time")
    local start_seconds=$(date +%s -d "$START_TIME")
    local end_seconds=$(date +%s -d "$END_TIME")
    
    if [ $current_seconds -ge $start_seconds ] && [ $current_seconds -le $end_seconds ]; then
        return 0  # True - within market hours
    else
        return 1  # False - outside market hours
    fi
}

# Function to get seconds until next market open
seconds_until_market_open() {
    local current_time=$(date +%H:%M)
    local current_seconds=$(date +%s -d "$current_time")
    local start_seconds=$(date +%s -d "$START_TIME")
    
    if [ $current_seconds -lt $start_seconds ]; then
        # Market opens today
        echo $((start_seconds - current_seconds))
    else
        # Market opens tomorrow
        local tomorrow_start=$(date +%s -d "tomorrow $START_TIME")
        echo $((tomorrow_start - current_seconds))
    fi
}

# Function to run the parallel update
run_update() {
    echo "----------------------------------------"
    echo "Running update at: $(date)"
    echo "----------------------------------------"
    
    # Set environment variable for number of workers
    export NUM_WORKERS=$NUM_WORKERS
    
    # Activate virtual environment if it exists
    if [ -d "../.venv" ]; then
        source ../.venv/bin/activate
    fi
    
    # Check if required files exist
    if [ ! -f "nifty500.txt" ]; then
        echo "ERROR: nifty500.txt not found!"
        return 1
    fi
    
    if [ ! -f "indices_list.txt" ]; then
        echo "ERROR: indices_list.txt not found!"
        return 1
    fi
    
    if [ ! -f ".env" ]; then
        echo "ERROR: .env file not found!"
        return 1
    fi
    
    # Run the parallel orchestrator
    if python3 parallel_daily_update.py; then
        echo "Update completed successfully at: $(date)"
    else
        echo "Update failed at: $(date)"
        return 1
    fi
}

# Main loop
while true; do
    if is_market_hours; then
        echo "Market is open - running update..."
        
        if run_update; then
            echo "Waiting $INTERVAL_MINUTES minutes until next update..."
            sleep $((INTERVAL_MINUTES * 60))
        else
            echo "Update failed, waiting 1 minute before retry..."
            sleep 60
        fi
    else
        wait_seconds=$(seconds_until_market_open)
        wait_hours=$((wait_seconds / 3600))
        wait_minutes=$(((wait_seconds % 3600) / 60))
        
        echo "Market is closed. Next market open in ${wait_hours}h ${wait_minutes}m"
        echo "Sleeping until market opens at $START_TIME..."
        
        # Sleep until 5 minutes before market open to be ready
        sleep_seconds=$((wait_seconds - 300))
        if [ $sleep_seconds -gt 0 ]; then
            sleep $sleep_seconds
        else
            sleep 60  # If less than 5 minutes, just wait 1 minute
        fi
    fi
done
