#!/usr/bin/env python3
"""
Parallel Historical Data Ingestion Script
Fetches and stores OHLCV data with multi-threading for faster processing
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
from io import StringIO
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from kiteconnect import KiteConnect
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)-10s] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_ingestion_parallel.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
DB_URI = os.getenv("DATABASE_URL")
KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_API_SECRET = os.getenv("KITE_API_SECRET")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

# Create engine with connection pooling
engine = create_engine(DB_URI, pool_size=20, max_overflow=40)

# Thread-local storage for Kite Connect instances
thread_local = threading.local()

def get_kite_instance():
    """Get thread-local Kite Connect instance"""
    if not hasattr(thread_local, 'kite'):
        thread_local.kite = KiteConnect(api_key=KITE_API_KEY)
        if KITE_ACCESS_TOKEN:
            thread_local.kite.set_access_token(KITE_ACCESS_TOKEN)
    return thread_local.kite

# Interval configurations
INTERVALS = ['1day', '60minute']

INTERVAL_LOOKBACK_YEARS = {
    '1minute': 1,
    '3minute': 2,
    '5minute': 2,
    '15minute': 3,
    '30minute': 3,
    '60minute': 7,
    '180minute': 7,
    '1day': 20,
    '1week': 20,
    '1month': 20,
}

CHUNK_DAYS_BY_INTERVAL = {
    '1minute': 7,
    '3minute': 10,
    '5minute': 14,
    '15minute': 30,
    '30minute': 45,
    '60minute': 90,
    '180minute': 180,
    '1day': 365,
    '1week': 365*3,
    '1month': 365*5,
}

# Rate limiting
rate_limit_lock = threading.Lock()
last_api_call = [time.time()]

def rate_limited_sleep():
    """Ensure minimum 0.25s between API calls (thread-safe)"""
    with rate_limit_lock:
        elapsed = time.time() - last_api_call[0]
        if elapsed < 0.25:
            time.sleep(0.25 - elapsed)
        last_api_call[0] = time.time()


def fetch_historical(instrument_token, from_date, to_date, interval='5minute'):
    """Fetch historical data from Kite Connect API (thread-safe)"""
    interval_map = {
        '1minute': 'minute',
        '3minute': '3minute',
        '5minute': '5minute',
        '15minute': '15minute',
        '30minute': '30minute',
        '60minute': '60minute',
        '180minute': '60minute',
        '1day': 'day',
        '1week': 'week',
        '1month': 'month',
    }
    
    kite_interval = interval_map.get(interval, interval)
    kite = get_kite_instance()
    
    try:
        from_dt = from_date if isinstance(from_date, datetime) else pd.to_datetime(from_date)
        to_dt = to_date if isinstance(to_date, datetime) else pd.to_datetime(to_date)
        
        rate_limited_sleep()  # Thread-safe rate limiting
        
        records = kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_dt,
            to_date=to_dt,
            interval=kite_interval,
            continuous=False,
            oi=True
        )
        
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        if 'date' in df.columns:
            df = df.rename(columns={'date': 'ts'})
        
        return df
        
    except Exception as e:
        err_str = str(e)
        if '503' in err_str or 'rate' in err_str.lower() or 'too many' in err_str.lower():
            logger.warning(f"Rate limit/503 error: {e}")
            time.sleep(1.0)  # Back off on rate limit
            raise
        logger.error(f"Error fetching historical data: {e}")
        return pd.DataFrame()


def df_to_postgres_copy(df, table_name, conn, columns=None, commit=True):
    """Fast bulk insert using COPY FROM STDIN"""
    buf = StringIO()
    df.to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur = conn.cursor()
    try:
        if columns:
            cols_str = ', '.join(columns)
            copy_sql = f"COPY {table_name} ({cols_str}) FROM STDIN WITH (FORMAT CSV)"
        else:
            copy_sql = f"COPY {table_name} FROM STDIN WITH (FORMAT CSV)"
        cur.copy_expert(copy_sql, buf)
        if commit:
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()


def ingest_candles_df(df_candles, instrument_token, tradingsymbol, exchange='NSE', interval='5minute'):
    """Normalize and bulk insert candles using temp table approach"""
    df = df_candles.copy()
    if df.empty:
        return 0

    df['tradingsymbol'] = tradingsymbol
    df['instrument_token'] = int(instrument_token)
    df['exchange'] = exchange
    df['interval'] = interval

    df['ts'] = pd.to_datetime(df['ts'])
    if df['ts'].dt.tz is None:
        df['ts'] = df['ts'].dt.tz_localize('UTC')

    for col in ['open', 'high', 'low', 'close']:
        df[col] = df[col].astype(float)

    df['volume'] = df.get('volume', pd.Series([0]*len(df))).fillna(0).astype(int)
    if 'oi' not in df.columns:
        df['oi'] = None

    df_to_copy = df[['instrument_token', 'tradingsymbol', 'exchange', 'interval', 'ts', 'open', 'high', 'low', 'close', 'volume', 'oi']]

    conn = psycopg2.connect(DB_URI)
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TEMP TABLE temp_ohlcv (
                instrument_token BIGINT,
                tradingsymbol TEXT,
                exchange TEXT,
                interval TEXT,
                ts TIMESTAMP WITH TIME ZONE,
                open NUMERIC(18,6),
                high NUMERIC(18,6),
                low NUMERIC(18,6),
                close NUMERIC(18,6),
                volume BIGINT,
                oi BIGINT
            ) ON COMMIT DROP
        """)
        
        columns = ['instrument_token', 'tradingsymbol', 'exchange', 'interval', 'ts', 'open', 'high', 'low', 'close', 'volume', 'oi']
        df_to_postgres_copy(df_to_copy, 'temp_ohlcv', conn, columns=columns, commit=False)
        
        cur.execute("""
            INSERT INTO ohlcv (instrument_token, tradingsymbol, exchange, interval, ts, open, high, low, close, volume, oi)
            SELECT instrument_token, tradingsymbol, exchange, interval, ts, open, high, low, close, volume, oi
            FROM temp_ohlcv
            ON CONFLICT (instrument_token, interval, ts) DO NOTHING
        """)
        
        inserted = cur.rowcount
        conn.commit()
        return inserted
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def make_date_chunks(start_dt, end_dt, chunk_days):
    """Yield date chunks"""
    cur = start_dt
    while cur < end_dt:
        nxt = min(cur + timedelta(days=chunk_days), end_dt)
        yield (cur, nxt)
        cur = nxt + timedelta(seconds=1)


def process_instrument_interval(instrument_token, tradingsymbol, interval, start_ts, end_ts,
                                chunk_days=None, max_retries=5):
    """Process one instrument + interval (thread-safe)"""
    chunk_days = chunk_days or CHUNK_DAYS_BY_INTERVAL.get(interval, 90)
    inserted_total = 0
    chunks_processed = 0
    chunks_failed = 0

    # Check/create job tracking
    with engine.begin() as conn:
        existing = conn.execute(text("""
            SELECT job_id, last_ingested_ts, status FROM ingest_jobs
            WHERE instrument_token = :it AND interval = :itv AND start_ts = :s AND end_ts = :e
            FOR UPDATE
        """), {"it": instrument_token, "itv": interval, "s": start_ts, "e": end_ts}).fetchone()

        if existing is None:
            r = conn.execute(text("""
                INSERT INTO ingest_jobs (instrument_token, tradingsymbol, interval, start_ts, end_ts, status)
                VALUES (:it, :sym, :itv, :s, :e, :status)
                RETURNING job_id
            """), {"it": instrument_token, "sym": tradingsymbol, "itv": interval, "s": start_ts, "e": end_ts, "status": "pending"})
            job_id = r.scalar()
            last_ingested_ts = None
            status = 'pending'
        else:
            job_id, last_ingested_ts, status = existing
            job_id = int(job_id)

    if status == 'done':
        logger.info(f"✓ {tradingsymbol} {interval} already complete")
        return {"inserted": 0, "chunks": 0, "status": "done", "symbol": tradingsymbol, "interval": interval}

    resume_after = last_ingested_ts if last_ingested_ts is not None else start_ts

    try:
        with engine.begin() as conn:
            conn.execute(text("UPDATE ingest_jobs SET status='running', updated_at=now(), last_error=NULL WHERE job_id=:jid"),
                         {"jid": job_id})

        chunks = list(make_date_chunks(resume_after, end_ts, chunk_days))
        
        for chunk_start, chunk_end in chunks:
            chunks_processed += 1
            success = False
            attempts = 0

            while not success and attempts < max_retries:
                attempts += 1
                try:
                    df_chunk = fetch_historical(instrument_token, chunk_start, chunk_end, interval=interval)
                    if df_chunk.empty:
                        logger.debug(f"{tradingsymbol} {interval}: Empty chunk {chunk_start.date()} -> {chunk_end.date()}")
                    else:
                        inserted = ingest_candles_df(df_chunk, instrument_token, tradingsymbol, exchange='NSE', interval=interval)
                        inserted_total += inserted
                        if inserted > 0:
                            logger.info(f"✓ {tradingsymbol} {interval}: Inserted {inserted} rows ({chunk_start.date()} -> {chunk_end.date()})")
                    
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE ingest_jobs SET last_ingested_ts = :ts, updated_at = now()
                            WHERE job_id = :jid
                        """), {"ts": chunk_end, "jid": job_id})
                    success = True
                except Exception as e:
                    logger.warning(f"✗ {tradingsymbol} {interval}: Attempt {attempts}/{max_retries} failed: {str(e)[:100]}")
                    if attempts < max_retries:
                        time.sleep(1.0 * attempts)
            
            if not success:
                chunks_failed += 1

        with engine.begin() as conn:
            conn.execute(text("UPDATE ingest_jobs SET status='done', updated_at=now() WHERE job_id=:jid"), {"jid": job_id})
        
        logger.info(f"✅ {tradingsymbol} {interval}: Complete - {inserted_total} rows inserted")
        return {"inserted": inserted_total, "chunks": chunks_processed, "failed": chunks_failed, "status": "done", "symbol": tradingsymbol, "interval": interval}
    except Exception as e:
        err = str(e)[:2000]
        with engine.begin() as conn:
            conn.execute(text("UPDATE ingest_jobs SET status='error', last_error=:err, updated_at=now() WHERE job_id=:jid"),
                         {"err": err, "jid": job_id})
        logger.error(f"❌ {tradingsymbol} {interval}: Failed - {e}")
        return {"inserted": inserted_total, "chunks": chunks_processed, "failed": chunks_failed, "status": "error", "symbol": tradingsymbol, "interval": interval}


def process_task(task):
    """Wrapper function for parallel processing"""
    instrument_token, tradingsymbol, interval, start_date, end_date, chunk_days, lookback_years = task
    
    result = process_instrument_interval(
        instrument_token=instrument_token,
        tradingsymbol=tradingsymbol,
        interval=interval,
        start_ts=start_date,
        end_ts=end_date,
        chunk_days=chunk_days,
        max_retries=5
    )
    
    result['years'] = lookback_years
    return result


def main():
    """Main orchestrator with parallel processing"""
    logger.info("=" * 80)
    logger.info("Starting Parallel Historical Data Ingestion")
    logger.info("=" * 80)
    
    # Configuration
    END_DATE = datetime.now(timezone.utc)
    NUM_WORKERS = int(os.getenv('NUM_WORKERS', '10'))  # Configurable via env
    
    logger.info(f"Using {NUM_WORKERS} parallel workers")
    
    # Get all instruments from database
    query = """
        SELECT instrument_token, tradingsymbol, exchange
        FROM instruments
        WHERE is_active = true
        ORDER BY tradingsymbol
    """
    
    with engine.connect() as conn:
        df_candidates = pd.read_sql(query, conn)
    
    logger.info(f"Found {len(df_candidates)} instruments in database")
    logger.info(f"Sample instruments: {', '.join(df_candidates['tradingsymbol'].head(10).tolist())}")
    
    # Build task list
    tasks = []
    for idx, r in df_candidates.iterrows():
        tkn = int(r['instrument_token'])
        sym = r['tradingsymbol']
        
        for itv in INTERVALS:
            lookback_years = INTERVAL_LOOKBACK_YEARS.get(itv, 3)
            start_date = END_DATE - timedelta(days=365*lookback_years)
            chunk_days = CHUNK_DAYS_BY_INTERVAL.get(itv, 90)
            
            tasks.append((tkn, sym, itv, start_date, END_DATE, chunk_days, lookback_years))
    
    logger.info(f"Total tasks to process: {len(tasks)}")
    
    # Process tasks in parallel
    summary = []
    completed = 0
    failed = 0
    
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit all tasks
        future_to_task = {executor.submit(process_task, task): task for task in tasks}
        
        # Progress bar
        with tqdm(total=len(tasks), desc="Overall Progress") as pbar:
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    summary.append(result)
                    
                    if result['status'] == 'done':
                        completed += 1
                    else:
                        failed += 1
                    
                    pbar.update(1)
                    pbar.set_postfix({
                        'completed': completed,
                        'failed': failed,
                        'current': f"{result['symbol']}-{result['interval']}"
                    })
                    
                except Exception as e:
                    logger.error(f"Task failed: {task[1]} {task[2]} - {e}")
                    failed += 1
                    pbar.update(1)
    
    # Summary report
    logger.info("\n" + "=" * 80)
    logger.info("INGESTION SUMMARY")
    logger.info("=" * 80)
    
    df_summary = pd.DataFrame(summary)
    
    logger.info(f"\nTotal instruments processed: {len(df_candidates)}")
    logger.info(f"Total intervals: {len(INTERVALS)}")
    logger.info(f"Total tasks: {len(tasks)}")
    logger.info(f"Total rows inserted: {df_summary['inserted'].sum():,}")
    logger.info(f"Successful jobs: {completed}")
    logger.info(f"Failed jobs: {failed}")
    
    # Save summary to CSV
    summary_file = f"ingestion_summary_parallel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_summary.to_csv(summary_file, index=False)
    logger.info(f"\nDetailed summary saved to: {summary_file}")
    
    # Print top performers
    if not df_summary.empty:
        logger.info("\nTop 10 by rows inserted:")
        top10 = df_summary.nlargest(10, 'inserted')[['symbol', 'interval', 'inserted']]
        logger.info("\n" + top10.to_string(index=False))
    
    logger.info("\n" + "=" * 80)
    logger.info("Parallel Ingestion Complete!")
    logger.info("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n\nIngestion interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n\nFatal error: {e}", exc_info=True)
        sys.exit(1)
