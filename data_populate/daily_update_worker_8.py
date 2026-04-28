#!/usr/bin/env python3
"""
Daily Update Worker Template
Individual worker process for parallel data ingestion
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

# Worker configuration (will be replaced by orchestrator)
WORKER_ID = 8
SYMBOLS_FILE = "worker_8_symbols.txt"

# Setup logging with worker-specific file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'worker_{WORKER_ID}_update.log'),
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

# Create engine
engine = create_engine(DB_URI)

# Initialize Kite Connect
kite = KiteConnect(api_key=KITE_API_KEY)
if KITE_ACCESS_TOKEN:
    kite.set_access_token(KITE_ACCESS_TOKEN)

# Interval configurations
INTERVALS = ['1day', '60minute']

CHUNK_DAYS_BY_INTERVAL = {
    '1day': 365,
    '60minute': 90,
}

def fetch_historical(instrument_token, from_date, to_date, interval='1day'):
    """Fetch historical data from Kite Connect API"""
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

    try:
        from_dt = from_date if isinstance(from_date, datetime) else pd.to_datetime(from_date)
        to_dt = to_date if isinstance(to_date, datetime) else pd.to_datetime(to_date)

        # Staggered rate limiting based on worker ID to avoid conflicts
        base_sleep = 0.25
        worker_offset = (WORKER_ID - 1) * 0.1  # 0.1s offset per worker
        time.sleep(base_sleep + worker_offset)

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
            logger.warning(f"Worker {WORKER_ID}: Rate limit/503 error: {e}")
            raise
        logger.error(f"Worker {WORKER_ID}: Error fetching historical data: {e}")
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

def ingest_candles_df(df_candles, instrument_token, tradingsymbol, exchange='NSE', interval='1day'):
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
        # Create temp table with worker-specific name to avoid conflicts
        temp_table = f"temp_ohlcv_worker_{WORKER_ID}"
        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cur.execute(f"""
            CREATE TEMP TABLE {temp_table} (
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
        df_to_postgres_copy(df_to_copy, temp_table, conn, columns=columns, commit=False)

        cur.execute(f"""
            INSERT INTO ohlcv (instrument_token, tradingsymbol, exchange, interval, ts, open, high, low, close, volume, oi)
            SELECT instrument_token, tradingsymbol, exchange, interval, ts, open, high, low, close, volume, oi
            FROM {temp_table}
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
                                chunk_days=None, max_retries=5, sleep_between_chunks=1.0,
                                progress_bar=None):
    """Process one instrument + interval with progress tracking"""
    chunk_days = chunk_days or CHUNK_DAYS_BY_INTERVAL.get(interval, 90)
    inserted_total = 0
    chunks_processed = 0
    chunks_failed = 0

    # Check/create job tracking with worker-specific job naming
    job_suffix = f"_worker_{WORKER_ID}"
    
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
            """), {"it": instrument_token, "sym": tradingsymbol + job_suffix, "itv": interval, "s": start_ts, "e": end_ts, "status": "pending"})
            job_id = r.scalar()
            last_ingested_ts = None
            status = 'pending'
        else:
            job_id, last_ingested_ts, status = existing
            job_id = int(job_id)

    if status == 'done':
        if progress_bar:
            progress_bar.write(f"Worker {WORKER_ID}: ✓ {tradingsymbol} {interval} already complete")
        return {"inserted": 0, "chunks": 0, "status": "done"}

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
                        if progress_bar:
                            progress_bar.write(f"Worker {WORKER_ID}: Empty chunk: {chunk_start.date()} -> {chunk_end.date()}")
                    else:
                        inserted = ingest_candles_df(df_chunk, instrument_token, tradingsymbol, exchange='NSE', interval=interval)
                        inserted_total += inserted
                        if progress_bar and inserted > 0:
                            progress_bar.write(f"Worker {WORKER_ID}: ✓ Inserted {inserted} rows: {chunk_start.date()} -> {chunk_end.date()}")

                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE ingest_jobs SET last_ingested_ts = :ts, updated_at = now()
                            WHERE job_id = :jid
                        """), {"ts": chunk_end, "jid": job_id})
                    success = True
                except Exception as e:
                    if progress_bar:
                        progress_bar.write(f"Worker {WORKER_ID}: ✗ Attempt {attempts}/{max_retries} failed: {str(e)[:100]}")
                    if attempts < max_retries:
                        # Exponential backoff with worker-specific jitter
                        sleep_time = (1.0 * attempts) + (WORKER_ID * 0.1)
                        time.sleep(sleep_time)

            if not success:
                chunks_failed += 1

            if progress_bar:
                progress_bar.update(1)
            
            # Worker-specific sleep to stagger API calls
            worker_sleep = sleep_between_chunks + (WORKER_ID * 0.2)
            time.sleep(worker_sleep)

        with engine.begin() as conn:
            conn.execute(text("UPDATE ingest_jobs SET status='done', updated_at=now() WHERE job_id=:jid"), {"jid": job_id})

        return {"inserted": inserted_total, "chunks": chunks_processed, "failed": chunks_failed, "status": "done"}
    except Exception as e:
        err = str(e)[:2000]
        with engine.begin() as conn:
            conn.execute(text("UPDATE ingest_jobs SET status='error', last_error=:err, updated_at=now() WHERE job_id=:jid"),
                         {"err": err, "jid": job_id})
        logger.error(f"Worker {WORKER_ID}: Job failed for {tradingsymbol} {interval}: {e}")
        return {"inserted": inserted_total, "chunks": chunks_processed, "failed": chunks_failed, "status": "error"}

def main():
    """Worker main function"""
    logger.info("=" * 60)
    logger.info(f"Starting Worker {WORKER_ID} - Daily Data Update")
    logger.info("=" * 60)

    # Configuration - fetch last 7 days to catch any missed data
    END_DATE = datetime.now(timezone.utc)
    START_DATE = END_DATE - timedelta(days=7)

    logger.info(f"Worker {WORKER_ID}: Update period: {START_DATE.date()} -> {END_DATE.date()}")

    # Read assigned symbols
    try:
        with open(SYMBOLS_FILE, 'r') as f:
            assigned_symbols = [line.strip() for line in f if line.strip()]
        logger.info(f"Worker {WORKER_ID}: Loaded {len(assigned_symbols)} assigned symbols")
    except FileNotFoundError:
        logger.error(f"Worker {WORKER_ID}: Symbols file {SYMBOLS_FILE} not found!")
        return

    if not assigned_symbols:
        logger.error(f"Worker {WORKER_ID}: No symbols to process!")
        return

    # Get instruments for the assigned symbols
    symbols_placeholder = ', '.join(['%s'] * len(assigned_symbols))
    query = f"""
        SELECT instrument_token, tradingsymbol, exchange
        FROM instruments
        WHERE tradingsymbol IN ({symbols_placeholder})
        ORDER BY tradingsymbol
    """

    with engine.connect() as conn:
        df_candidates = pd.read_sql(query, conn, params=tuple(assigned_symbols))

    logger.info(f"Worker {WORKER_ID}: Found {len(df_candidates)}/{len(assigned_symbols)} symbols in database")

    if df_candidates.empty:
        logger.error(f"Worker {WORKER_ID}: No instruments found in database!")
        return

    summary = []
    total_tasks = len(df_candidates) * len(INTERVALS)

    with tqdm(total=total_tasks, desc=f"Worker {WORKER_ID} Progress", position=0) as pbar_overall:
        for idx, r in df_candidates.iterrows():
            tkn = int(r['instrument_token'])
            sym = r['tradingsymbol']

            logger.info(f"Worker {WORKER_ID}: Processing {sym} ({tkn}) - {idx+1}/{len(df_candidates)}")

            for itv in INTERVALS:
                chunk_days = CHUNK_DAYS_BY_INTERVAL.get(itv, 90)
                total_days = (END_DATE - START_DATE).days
                total_chunks = max(1, total_days // chunk_days)

                logger.info(f"Worker {WORKER_ID}: {sym} - {itv}: updating ({START_DATE.date()} -> {END_DATE.date()})")

                with tqdm(total=total_chunks, desc=f"  {itv}", position=1, leave=False) as pbar_interval:
                    res = process_instrument_interval(
                        instrument_token=tkn,
                        tradingsymbol=sym,
                        interval=itv,
                        start_ts=START_DATE,
                        end_ts=END_DATE,
                        chunk_days=chunk_days,
                        max_retries=5,
                        sleep_between_chunks=1.0,
                        progress_bar=pbar_interval
                    )

                summary.append({
                    'worker_id': WORKER_ID,
                    'symbol': sym,
                    'interval': itv,
                    'status': res['status'],
                    'inserted': res.get('inserted', 0),
                    'chunks': res.get('chunks', 0),
                    'failed': res.get('failed', 0)
                })

                pbar_overall.update(1)
                
                # Worker-specific delay between symbols
                time.sleep(2.0 + (WORKER_ID * 0.3))

    # Summary report
    logger.info("\n" + "=" * 60)
    logger.info(f"Worker {WORKER_ID} - UPDATE SUMMARY")
    logger.info("=" * 60)

    df_summary = pd.DataFrame(summary)

    logger.info(f"Worker {WORKER_ID}: Symbols processed: {len(df_candidates)}")
    logger.info(f"Worker {WORKER_ID}: Total intervals: {len(INTERVALS)}")
    logger.info(f"Worker {WORKER_ID}: Total rows inserted: {df_summary['inserted'].sum():,}")
    logger.info(f"Worker {WORKER_ID}: Successful updates: {len(df_summary[df_summary['status'] == 'done'])}")
    logger.info(f"Worker {WORKER_ID}: Failed updates: {len(df_summary[df_summary['status'] == 'error'])}")

    # Save worker-specific summary
    summary_file = f"worker_{WORKER_ID}_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_summary.to_csv(summary_file, index=False)
    logger.info(f"Worker {WORKER_ID}: Summary saved to: {summary_file}")

    logger.info("\n" + "=" * 60)
    logger.info(f"Worker {WORKER_ID} Complete!")
    logger.info("=" * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning(f"\n\nWorker {WORKER_ID} interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n\nWorker {WORKER_ID} fatal error: {e}", exc_info=True)
        sys.exit(1)
