#!/usr/bin/env python3
"""
Parallel Daily Update Script - Main Orchestrator
Distributes stock data ingestion across multiple worker processes
"""

import os
import sys
import time
import logging
import multiprocessing as mp
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parallel_daily_update.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

def get_all_symbols():
    """Get all symbols to process from database"""
    try:
        with engine.connect() as conn:
            # Fetch all active instruments from database
            query = """
                SELECT DISTINCT tradingsymbol, instrument_token, exchange
                FROM instruments 
                WHERE is_active = true
                ORDER BY tradingsymbol
            """
            result = conn.execute(text(query))
            instruments = result.fetchall()
            
            if not instruments:
                logger.error("No active instruments found in database!")
                return []
            
            # Extract just the trading symbols for processing
            symbols = [row[0] for row in instruments]
            logger.info(f"Loaded {len(symbols)} active instruments from database")
            
            # Log some sample symbols
            sample_symbols = symbols[:10] if len(symbols) > 10 else symbols
            logger.info(f"Sample symbols: {', '.join(sample_symbols)}")
            
            return symbols
            
    except Exception as e:
        logger.error(f"Error fetching symbols from database: {e}")
        return []

def create_symbol_chunks(symbols, num_workers):
    """Split symbols into chunks for parallel processing"""
    chunk_size = len(symbols) // num_workers
    remainder = len(symbols) % num_workers
    
    chunks = []
    start = 0
    
    for i in range(num_workers):
        # Add one extra symbol to first 'remainder' chunks
        current_chunk_size = chunk_size + (1 if i < remainder else 0)
        end = start + current_chunk_size
        
        if start < len(symbols):
            chunk = symbols[start:end]
            chunks.append({
                'worker_id': i + 1,
                'symbols': chunk,
                'start_idx': start + 1,  # 1-based indexing for display
                'end_idx': end
            })
        start = end
    
    return chunks

def run_worker_process(worker_id, symbols, start_idx, end_idx):
    """Run a single worker process"""
    logger.info(f"Worker {worker_id}: Processing symbols {start_idx}-{end_idx} ({len(symbols)} symbols)")
    
    # Create worker-specific script
    worker_script = f"daily_update_worker_{worker_id}.py"
    
    # Write symbols to worker-specific file
    symbols_file = f"worker_{worker_id}_symbols.txt"
    with open(symbols_file, 'w') as f:
        for symbol in symbols:
            f.write(f"{symbol}\n")
    
    # Run the worker script
    cmd = f"python3 {worker_script}"
    logger.info(f"Worker {worker_id}: Starting with command: {cmd}")
    
    start_time = time.time()
    exit_code = os.system(cmd)
    end_time = time.time()
    
    duration = end_time - start_time
    logger.info(f"Worker {worker_id}: Completed in {duration:.2f} seconds with exit code {exit_code}")
    
    # Cleanup
    if os.path.exists(symbols_file):
        os.remove(symbols_file)
    
    return {
        'worker_id': worker_id,
        'exit_code': exit_code,
        'duration': duration,
        'symbols_processed': len(symbols)
    }

def create_worker_scripts(chunks):
    """Create individual worker scripts"""
    base_script_path = Path(__file__).parent / "daily_update_worker_template.py"
    
    for chunk in chunks:
        worker_id = chunk['worker_id']
        worker_script = f"daily_update_worker_{worker_id}.py"
        
        # Read the template and customize it
        with open("daily_update_worker_template.py", 'r') as f:
            template_content = f.read()
        
        # Replace placeholders
        worker_content = template_content.replace(
            "WORKER_ID_PLACEHOLDER", str(worker_id)
        ).replace(
            "SYMBOLS_FILE_PLACEHOLDER", f"worker_{worker_id}_symbols.txt"
        )
        
        with open(worker_script, 'w') as f:
            f.write(worker_content)
        
        logger.info(f"Created worker script: {worker_script}")

def main():
    """Main orchestrator"""
    logger.info("=" * 80)
    logger.info("Starting PARALLEL Daily Data Update")
    logger.info("=" * 80)
    
    # Configuration
    NUM_WORKERS = int(os.getenv("NUM_WORKERS", "4"))  # Default 4 workers
    logger.info(f"Using {NUM_WORKERS} parallel workers")
    
    # Get all symbols from database
    all_symbols = get_all_symbols()
    if not all_symbols:
        logger.error("No symbols to process! Check database connection and ensure instruments table has active records.")
        return
    
    logger.info(f"Total symbols to process: {len(all_symbols)}")
    
    # Create chunks
    chunks = create_symbol_chunks(all_symbols, NUM_WORKERS)
    
    logger.info("\nWork distribution:")
    for chunk in chunks:
        logger.info(f"Worker {chunk['worker_id']}: symbols {chunk['start_idx']}-{chunk['end_idx']} ({len(chunk['symbols'])} symbols)")
    
    # Create worker scripts
    create_worker_scripts(chunks)
    
    # Start parallel execution
    logger.info(f"\nStarting {NUM_WORKERS} parallel workers...")
    start_time = time.time()
    
    with mp.Pool(processes=NUM_WORKERS) as pool:
        # Submit all worker tasks
        results = []
        for chunk in chunks:
            result = pool.apply_async(
                run_worker_process,
                (chunk['worker_id'], chunk['symbols'], chunk['start_idx'], chunk['end_idx'])
            )
            results.append(result)
        
        # Wait for all workers to complete
        worker_results = []
        for result in results:
            try:
                worker_result = result.get(timeout=3600)  # 1 hour timeout per worker
                worker_results.append(worker_result)
            except mp.TimeoutError:
                logger.error("Worker timed out after 1 hour")
                worker_results.append({'worker_id': 'unknown', 'exit_code': -1, 'duration': 3600, 'symbols_processed': 0})
    
    end_time = time.time()
    total_duration = end_time - start_time
    
    # Summary report
    logger.info("\n" + "=" * 80)
    logger.info("PARALLEL UPDATE SUMMARY")
    logger.info("=" * 80)
    
    successful_workers = sum(1 for r in worker_results if r['exit_code'] == 0)
    failed_workers = len(worker_results) - successful_workers
    total_symbols_processed = sum(r['symbols_processed'] for r in worker_results)
    
    logger.info(f"Total execution time: {total_duration:.2f} seconds ({total_duration/60:.1f} minutes)")
    logger.info(f"Successful workers: {successful_workers}/{NUM_WORKERS}")
    logger.info(f"Failed workers: {failed_workers}")
    logger.info(f"Total symbols processed: {total_symbols_processed}")
    
    if successful_workers > 0:
        avg_duration = sum(r['duration'] for r in worker_results if r['exit_code'] == 0) / successful_workers
        logger.info(f"Average worker duration: {avg_duration:.2f} seconds")
        
        speedup = avg_duration / (total_duration / NUM_WORKERS) if total_duration > 0 else 0
        logger.info(f"Parallel speedup: {speedup:.2f}x")
    
    logger.info("\nWorker details:")
    for result in worker_results:
        status = "✓ SUCCESS" if result['exit_code'] == 0 else "✗ FAILED"
        logger.info(f"Worker {result['worker_id']}: {status} - {result['duration']:.2f}s - {result['symbols_processed']} symbols")
    
    # Cleanup worker scripts
    for chunk in chunks:
        worker_script = f"daily_update_worker_{chunk['worker_id']}.py"
        if os.path.exists(worker_script):
            os.remove(worker_script)
    
    logger.info("\n" + "=" * 80)
    logger.info("Parallel Daily Update Complete!")
    logger.info("=" * 80)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n\nParallel update interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n\nFatal error in parallel update: {e}", exc_info=True)
        sys.exit(1)
