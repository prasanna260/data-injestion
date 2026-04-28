#!/bin/bash

START_TIME="09:15"
END_TIME="16:00"

# Convert times to seconds since epoch (today)
start_ts=$(date -d "$(date +%F) $START_TIME" +%s)
end_ts=$(date -d "$(date +%F) $END_TIME" +%s)

echo "Daily runner started at $(date)"

while true; do
    now_ts=$(date +%s)

    # If before start time, wait until 09:15
    if [ "$now_ts" -lt "$start_ts" ]; then
        sleep $((start_ts - now_ts))
        continue
    fi

    # If after end time, stop running for today
    if [ "$now_ts" -ge "$end_ts" ]; then
        echo "End time reached. Exiting at $(date)"
        break
    fi

    echo "Starting daily_update.py at $(date)"
    python3 daily_update.py
    echo "Finished daily_update.py at $(date)"

    # After script finishes, check time again
    now_ts=$(date +%s)
    if [ "$now_ts" -ge "$end_ts" ]; then
        echo "Finished after end time. No more runs today."
        break
    fi

    # Optional small sleep to avoid immediate restart (adjust if needed)
    sleep 5
done
