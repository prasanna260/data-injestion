#!/bin/bash

# Parallel Daily Update Runner
# Usage: ./run_parallel_update.sh [num_workers]

set -e

# Configuration
NUM_WORKERS=${1:-4}  # Default to 4 workers if not specified
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=========================================="
echo "Parallel Daily Update Runner"
echo "=========================================="
echo "Workers: $NUM_WORKERS"
echo "Script Directory: $SCRIPT_DIR"
echo "Time: $(date)"
echo "=========================================="

# Change to script directory
cd "$SCRIPT_DIR"

# Set environment variable for number of workers
export NUM_WORKERS=$NUM_WORKERS

# Activate virtual environment if it exists
if [ -d "../.venv" ]; then
    echo "Activating virtual environment..."
    source ../.venv/bin/activate
fi

# Check if required files exist
if [ ! -f ".env" ]; then
    echo "ERROR: .env file not found!"
    exit 1
fi

# Check database connectivity
echo "Checking database connectivity..."
python3 -c "
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DB_URI = os.getenv('DATABASE_URL')
if not DB_URI:
    print('ERROR: DATABASE_URL not found in .env file!')
    exit(1)

try:
    engine = create_engine(DB_URI)
    with engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM instruments WHERE is_active = true'))
        count = result.scalar()
        print(f'✓ Database connected successfully. Found {count} active instruments.')
except Exception as e:
    print(f'ERROR: Database connection failed: {e}')
    exit(1)
"

if [ $? -ne 0 ]; then
    echo "Database connectivity check failed!"
    exit 1
fi

# Run the parallel orchestrator
echo "Starting parallel daily update with $NUM_WORKERS workers..."
python parallel_daily_update.py

# Check exit code
if [ $? -eq 0 ]; then
    echo "=========================================="
    echo "Parallel Daily Update COMPLETED Successfully!"
    echo "Time: $(date)"
    echo "=========================================="
else
    echo "=========================================="
    echo "Parallel Daily Update FAILED!"
    echo "Time: $(date)"
    echo "=========================================="
    exit 1
fi
