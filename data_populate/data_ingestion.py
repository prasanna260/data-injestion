#!/usr/bin/env python3
"""
Optimized Historical Data Ingestion Engine for Kite Connect

Features:
- Correct chunking per interval (no gaps / no overlaps)
- Robust resume: job records the max timestamp actually inserted
- Single SQLAlchemy engine for pooling + raw psycopg2 COPY for fast bulk loads
- 3-hour aggregation (if 180minute requested) via 60-minute aggregation
- Automatic index / table checks (creates indexes if missing)
- Rate-limit-aware exponential backoff + retries
- Clean progress reporting with tqdm (logs still go to file)
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Iterator, Tuple, Optional

import pandas as pd
from kiteconnect import KiteConnect
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from tqdm import tqdm

# -----------------------
# CONFIG & ENV
# -----------------------
load_dotenv()

DB_URI = os.getenv("RAILWAY_DB_URI") or os.getenv("DATABASE_URL") or "postgresql://postgres:password@localhost:5432/postgres"
KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_API_SECRET = os.getenv("KITE_API_SECRET")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

# Limits & tuning
DEFAULT_LIMIT_INSTRUMENTS = 100  # Fixed batch size
OFFSET_INSTRUMENTS = 0  # Skip first N instruments for batching (change this value for different batches)
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BASE_SLEEP = float(os.getenv("BASE_SLEEP", "1.0"))  # base sleep between attempts
SLEEP_BETWEEN_CHUNKS = float(os.getenv("SLEEP_BETWEEN_CHUNKS", "0.5"))
RATE_LIMIT_SLEEP = float(os.getenv("RATE_LIMIT_SLEEP", "0.25"))

# Intervals
INTERVALS = [
    '1minute', '3minute', '5minute', '15minute', '30minute',
    '60minute', '180minute', '1day', '1week', '1month'
]

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
    '180minute': 90,   # We'll fetch 60min chunks then aggregate
    '1day': 365,
    '1week': 365 * 3,
    '1month': 365 * 5,
}

# Mapping to Kite interval string (what kite.historical_data expects)
KITE_INTERVAL_MAP = {
    '1minute': 'minute',
    '3minute': '3minute',
    '5minute': '5minute',
    '15minute': '15minute',
    '30minute': '30minute',
    '60minute': '60minute',
    # '180minute' handled by aggregation below
    '1day': 'day',
    '1week': 'week',
    '1month': 'month',
}

# Unit step for advancing chunks: returns timedelta to add to chunk end to get next start
def interval_step_delta(interval: str) -> timedelta:
    if interval.endswith('minute'):
        # parse minutes prefix (e.g., '180minute' -> 180)
        num = int(interval.replace('minute', ''))
        return timedelta(minutes=num)
    if interval == '1day':
        return timedelta(days=1)
    if interval == '1week':
        return timedelta(weeks=1)
    if interval == '1month':
        # month is variable; we'll step by one day for chunking boundaries to avoid missing days
        return timedelta(days=1)
    return timedelta(minutes=1)

# -----------------------
# LOGGING
# -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("ingestion_engine.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ingest_engine")

# -----------------------
# DB & KITE CLIENT INIT
# -----------------------
engine: Engine = create_engine(DB_URI, pool_size=10, max_overflow=20, pool_pre_ping=True)

kite = KiteConnect(api_key=KITE_API_KEY)
if KITE_ACCESS_TOKEN:
    kite.set_access_token(KITE_ACCESS_TOKEN)

# -----------------------
# SCHEMA / INDEX HELPERS
# -----------------------
def ensure_db_indexes_and_tables():
    """Create recommended indexes and basic tables if they don't exist."""
    with engine.begin() as conn:
        # base ohlcv table (assume user has created but create minimal if missing)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            instrument_token BIGINT NOT NULL,
            tradingsymbol TEXT,
            exchange TEXT,
            interval TEXT,
            ts TIMESTAMP WITH TIME ZONE NOT NULL,
            open NUMERIC(18,6),
            high NUMERIC(18,6),
            low NUMERIC(18,6),
            close NUMERIC(18,6),
            volume BIGINT,
            oi BIGINT,
            PRIMARY KEY (instrument_token, interval, ts)
        );
        """))
        # ingest_jobs table
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ingest_jobs (
            job_id BIGSERIAL PRIMARY KEY,
            instrument_token BIGINT NOT NULL,
            tradingsymbol TEXT,
            interval TEXT NOT NULL,
            start_ts TIMESTAMP WITH TIME ZONE,
            end_ts TIMESTAMP WITH TIME ZONE,
            last_ingested_ts TIMESTAMP WITH TIME ZONE,
            status TEXT,
            last_error TEXT,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
        );
        """))
        # ensure index on ingest_jobs for quick lookups
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_ingest_jobs_instrument_interval
        ON ingest_jobs (instrument_token, interval);
        """))
        # ohlcv primary key exists above; if not, add unique index
        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlcv_unique
        ON ohlcv (instrument_token, interval, ts);
        """))

# -----------------------
# HELPER: CHUNK GENERATOR (no gaps/no overlaps)
# -----------------------
def make_date_chunks(start_dt: datetime, end_dt: datetime, chunk_days: int, interval: str) -> Iterator[Tuple[datetime, datetime]]:
    """
    Yields (chunk_start_inclusive, chunk_end_inclusive) tuples.
    Advances next chunk start by smallest unit appropriate to interval (minute/day).
    Ensures no gaps and no overlaps at common minute/day boundaries.
    """
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)

    cur = start_dt
    # chunk length in days may be large; compute chunk end as cur + chunk_days - 1 unit
    while cur <= end_dt:
        nxt = cur + timedelta(days=chunk_days) - timedelta(seconds=1)  # make inclusive end
        if nxt > end_dt:
            nxt = end_dt
        yield (cur, nxt)
        # compute next start: move forward by single step of interval_unit to avoid minute gaps
        step = interval_step_delta(interval)
        # For minute-based intervals, step might be multiple minutes (e.g., 180minute -> 180)
        # Next start should be nxt + smallest unit (1 minute for minute intervals, 1 day for day intervals)
        if interval.endswith('minute'):
            cur = nxt + timedelta(minutes=1)
        elif interval == '1day':
            cur = (nxt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            # for week/month and others, advance by 1 day to be safe
            cur = nxt + timedelta(days=1)

# -----------------------
# HELPER: FETCH + AGGREGATION
# -----------------------
def fetch_historical_raw(instrument_token: int, from_date: datetime, to_date: datetime, interval: str) -> pd.DataFrame:
    """
    Fetches raw historical data from Kite Connect.
    Returns DataFrame with columns: date/ts, open, high, low, close, volume, oi (if available).
    """
    kite_interval = KITE_INTERVAL_MAP.get(interval)
    if not kite_interval:
        raise ValueError(f"Interval {interval} not supported directly by Kite (handled separately if aggregation required).")

    # Ensure datetime objects
    from_dt = from_date if isinstance(from_date, datetime) else pd.to_datetime(from_date)
    to_dt = to_date if isinstance(to_date, datetime) else pd.to_datetime(to_date)

    # Kite API call
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(RATE_LIMIT_SLEEP)
            records = kite.historical_data(
                instrument_token=int(instrument_token),
                from_date=from_dt,
                to_date=to_dt,
                interval=kite_interval,
                continuous=False,
                oi=True
            )
            if not records:
                return pd.DataFrame()
            df = pd.DataFrame(records)
            # normalize
            if 'date' in df.columns:
                df = df.rename(columns={'date': 'ts'})
            df['ts'] = pd.to_datetime(df['ts'], utc=True)
            # ensure columns exist
            for c in ['open', 'high', 'low', 'close', 'volume', 'oi']:
                if c not in df.columns:
                    df[c] = None
            # cast numeric
            for c in ['open', 'high', 'low', 'close']:
                df[c] = pd.to_numeric(df[c], errors='coerce')
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype('int64')
            if 'oi' in df.columns:
                df['oi'] = pd.to_numeric(df['oi'], errors='coerce').astype('Float64')
            return df
        except Exception as e:
            err = str(e).lower()
            logger.warning(f"Fetch attempt {attempt}/{MAX_RETRIES} failed for {instrument_token} {from_dt} -> {to_dt}: {e}")
            if '503' in err or 'rate' in err or 'too many' in err:
                # aggressive backoff on rate-limit
                sleep_for = BASE_SLEEP * (2 ** (attempt - 1))
                logger.info(f"Rate-limited; sleeping {sleep_for}s before retry")
                time.sleep(sleep_for)
            else:
                # short backoff for other errors
                time.sleep(BASE_SLEEP * attempt)
            if attempt == MAX_RETRIES:
                raise
    return pd.DataFrame()

def fetch_and_maybe_aggregate(instrument_token: int, start_dt: datetime, end_dt: datetime, interval: str) -> pd.DataFrame:
    """
    If interval != '180minute' or supported by Kite, fetch directly.
    If interval == '180minute', fetch 60minute data for the same span then aggregate into 3-hour candles.
    """
    if interval == '180minute':
        # fetch 60min data for the span and aggregate into 3-hour groups aligned to midnight
        df60 = fetch_historical_raw(instrument_token, start_dt, end_dt, '60minute')
        if df60.empty:
            return pd.DataFrame()
        # Convert ts to UTC and floor to 3-hour bins aligned at 0:00
        df60['ts'] = pd.to_datetime(df60['ts'], utc=True)
        # compute 3-hour bucket start
        def bucket_3h(ts):
            minute = (ts.hour // 3) * 3
            return ts.replace(hour=minute, minute=0, second=0, microsecond=0)
        df60['bucket'] = df60['ts'].apply(bucket_3h)
        agg = df60.groupby('bucket').agg(
            open=('open', 'first'),
            high=('high', 'max'),
            low=('low', 'min'),
            close=('close', 'last'),
            volume=('volume', 'sum'),
            oi=('oi', 'last')
        ).reset_index().rename(columns={'bucket': 'ts'})
        agg['ts'] = pd.to_datetime(agg['ts'], utc=True)
        return agg
    else:
        return fetch_historical_raw(instrument_token, start_dt, end_dt, interval)

# -----------------------
# BULK COPY
# -----------------------
def df_to_postgres_copy_via_engine(df: pd.DataFrame, table_name: str, conn, columns: Optional[list] = None, commit: bool = True):
    """
    Uses raw psycopg2 connection available via SQLAlchemy connection to perform COPY FROM STDIN.
    `conn` is an SQLAlchemy connection object from engine.raw_connection() or engine.connect().connection
    """
    if df.empty:
        return 0
    buf = StringIO()
    # ensure ordering of columns
    if columns is None:
        columns = list(df.columns)
    df_to_csv = df[columns].copy()
    # Pandas will write NaN as empty; we want Postgres NULL literal for COPY -> use \N
    df_to_csv = df_to_csv.fillna('\\N')
    df_to_csv.to_csv(buf, index=False, header=False)
    buf.seek(0)

    raw_conn = conn
    # If the SQLAlchemy connection was passed, extract the underlying raw connection
    # If it's already a raw connection (psycopg2), use it directly
    cur = raw_conn.cursor()
    try:
        cols_str = ', '.join(columns)
        copy_sql = f"COPY {table_name} ({cols_str}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
        cur.copy_expert(copy_sql, buf)
        if commit:
            raw_conn.commit()
        return cur.rowcount if hasattr(cur, 'rowcount') else -1
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        cur.close()

# -----------------------
# INGESTION: normalize + write
# -----------------------
def ingest_candles_df(df_candles: pd.DataFrame, instrument_token: int, tradingsymbol: str, exchange='NSE', interval='5minute', conn_raw=None) -> int:
    """
    Normalize the df and insert into ohlcv table using temp table + COPY + INSERT ... ON CONFLICT DO NOTHING.
    Requires a raw psycopg2 connection object `conn_raw`.
    Returns number of inserted rows (approximate; when using COPY, rowcount may be -1).
    """
    if df_candles.empty:
        return 0

    df = df_candles.copy()
    # normalize ts
    df['ts'] = pd.to_datetime(df['ts'], utc=True)
    # add metadata
    df['instrument_token'] = int(instrument_token)
    df['tradingsymbol'] = tradingsymbol
    df['exchange'] = exchange
    df['interval'] = interval

    # ensure columns and dtypes
    for col in ['open', 'high', 'low', 'close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['volume'] = pd.to_numeric(df.get('volume', 0), errors='coerce').fillna(0).astype('int64')
    if 'oi' in df.columns:
        # convert to integer where possible
        df['oi'] = pd.to_numeric(df['oi'], errors='coerce').astype('Float64')
    else:
        df['oi'] = pd.NA

    # prepare df for copy: column order must match temp table
    df_to_copy = df[['instrument_token', 'tradingsymbol', 'exchange', 'interval', 'ts', 'open', 'high', 'low', 'close', 'volume', 'oi']].copy()
    # Use a transaction from the engine to create a temp table and copy into it (temp table lives in this session)
    # We'll use SQLAlchemy engine.raw_connection() to obtain a psycopg2 connection for COPY
    raw_conn = conn_raw
    cur = raw_conn.cursor()
    try:
        # Create temp table
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
                oi NUMERIC
            ) ON COMMIT DROP
        """)
        raw_conn.commit()

        # COPY into temp table
        inserted_rows = df_to_postgres_copy_via_engine(df_to_copy, 'temp_ohlcv', raw_conn, columns=list(df_to_copy.columns), commit=True)

        # Then upsert into main table
        cur.execute("""
            INSERT INTO ohlcv (instrument_token, tradingsymbol, exchange, interval, ts, open, high, low, close, volume, oi)
            SELECT instrument_token, tradingsymbol, exchange, interval, ts, open, high, low, close, volume, oi
            FROM temp_ohlcv
            ON CONFLICT (instrument_token, interval, ts) DO UPDATE
              SET open = EXCLUDED.open,
                  high = EXCLUDED.high,
                  low = EXCLUDED.low,
                  close = EXCLUDED.close,
                  volume = EXCLUDED.volume,
                  oi = EXCLUDED.oi
            WHERE ohlcv.instrument_token = EXCLUDED.instrument_token
              AND ohlcv.interval = EXCLUDED.interval
              AND ohlcv.ts = EXCLUDED.ts
        """)
        raw_conn.commit()
        # Note: rowcount for the INSERT may be -1 depending on driver; we'll attempt to return len(df)
        return len(df_to_copy)
    except Exception as e:
        raw_conn.rollback()
        logger.exception("Error ingesting candles df")
        raise
    finally:
        cur.close()

# -----------------------
# JOB PROCESSING
# -----------------------
def process_instrument_interval(instrument_token: int, tradingsymbol: str, interval: str,
                                start_ts: datetime, end_ts: datetime, chunk_days: Optional[int] = None,
                                progress_bar=None) -> dict:
    """
    Process ingestion for a single instrument & interval. Returns summary dict.
    Ensures single job row per (instrument, interval, start_ts, end_ts) and supports resume.
    """
    chunk_days = chunk_days or CHUNK_DAYS_BY_INTERVAL.get(interval, 90)
    inserted_total = 0
    chunks_processed = 0
    chunks_failed = 0

    # Ensure job row exists and get last_ingested_ts
    with engine.begin() as conn:
        # try to find existing job
        q = conn.execute(text("""
            SELECT job_id, last_ingested_ts, status
            FROM ingest_jobs
            WHERE instrument_token = :it AND interval = :itv AND start_ts = :s AND end_ts = :e
            FOR UPDATE
        """), {"it": instrument_token, "itv": interval, "s": start_ts, "e": end_ts}).fetchone()

        if q is None:
            r = conn.execute(text("""
                INSERT INTO ingest_jobs (instrument_token, tradingsymbol, interval, start_ts, end_ts, status)
                VALUES (:it, :sym, :itv, :s, :e, :status)
                RETURNING job_id
            """), {"it": instrument_token, "sym": tradingsymbol, "itv": interval, "s": start_ts, "e": end_ts, "status": "pending"})
            job_id = int(r.scalar())
            last_ingested_ts = None
            status = "pending"
        else:
            job_id, last_ingested_ts, status = q
            job_id = int(job_id)

    if status == 'done':
        if progress_bar:
            progress_bar.write(f"✓ {tradingsymbol} {interval} already complete")
        return {"inserted": 0, "chunks": 0, "failed": 0, "status": "done"}

    resume_after = last_ingested_ts if last_ingested_ts is not None else start_ts

    # set job to running
    with engine.begin() as conn:
        conn.execute(text("UPDATE ingest_jobs SET status='running', updated_at=now(), last_error=NULL WHERE job_id=:jid"),
                     {"jid": job_id})

    # Pre-open a raw DB connection to reuse across chunks (to keep temp table context stable per call)
    raw_conn = engine.raw_connection()

    try:
        chunks = list(make_date_chunks(resume_after, end_ts, chunk_days, interval))
        total_chunks = len(chunks)
        if progress_bar:
            progress_bar.reset(total=total_chunks)

        for chunk_start, chunk_end in chunks:
            chunks_processed += 1
            attempts = 0
            success = False

            while attempts < MAX_RETRIES and not success:
                attempts += 1
                try:
                    # Fetch/aggregate
                    df_chunk = fetch_and_maybe_aggregate(instrument_token, chunk_start, chunk_end, interval)
                    if df_chunk.empty:
                        if progress_bar:
                            progress_bar.write(f"  Empty chunk: {chunk_start.isoformat()} -> {chunk_end.isoformat()}")
                    else:
                        # Insert into DB using same raw_conn
                        inserted = ingest_candles_df(df_chunk, instrument_token, tradingsymbol, exchange='NSE', interval=interval, conn_raw=raw_conn)
                        inserted_total += inserted
                        if progress_bar:
                            progress_bar.write(f"  ✓ Inserted {inserted} rows: {chunk_start.date()} -> {chunk_end.date()}")

                    # update last_ingested_ts to max actual ts inserted OR chunk_end if empty
                    last_ts = None
                    if not df_chunk.empty:
                        last_ts = df_chunk['ts'].max()
                    else:
                        last_ts = chunk_end

                    # Persist last_ingested_ts in ingest_jobs
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE ingest_jobs SET last_ingested_ts = :ts, updated_at = now()
                            WHERE job_id = :jid
                        """), {"ts": last_ts, "jid": job_id})

                    success = True
                except Exception as e:
                    # log and backoff
                    logger.exception(f"Attempt {attempts} failed for {tradingsymbol} {interval} chunk {chunk_start} -> {chunk_end}")
                    if attempts < MAX_RETRIES:
                        backoff = BASE_SLEEP * (2 ** (attempts - 1))
                        time.sleep(backoff)
                    else:
                        chunks_failed += 1
                finally:
                    time.sleep(SLEEP_BETWEEN_CHUNKS)

            if progress_bar:
                progress_bar.update(1)

        # mark job done
        with engine.begin() as conn:
            conn.execute(text("UPDATE ingest_jobs SET status='done', updated_at=now() WHERE job_id=:jid"), {"jid": job_id})

        return {"inserted": inserted_total, "chunks": chunks_processed, "failed": chunks_failed, "status": "done"}
    except Exception as e:
        logger.exception(f"Job failed for {tradingsymbol} {interval}")
        with engine.begin() as conn:
            conn.execute(text("UPDATE ingest_jobs SET status='error', last_error=:err, updated_at=now() WHERE job_id=:jid"),
                         {"err": str(e)[:2000], "jid": job_id})
        return {"inserted": inserted_total, "chunks": chunks_processed, "failed": chunks_failed, "status": "error"}
    finally:
        try:
            raw_conn.close()
        except Exception:
            pass

# -----------------------
# MAIN ORCHESTRATOR
# -----------------------
def main():
    logger.info("=" * 80)
    logger.info("Starting Optimized Historical Data Ingestion")
    logger.info("=" * 80)

    ensure_db_indexes_and_tables()

    LIMIT_INSTRUMENTS = DEFAULT_LIMIT_INSTRUMENTS
    OFFSET_INSTRUMENTS = int(os.getenv("OFFSET_INSTRUMENTS", "0"))  # Allow override per run
    END_DATE = datetime.now(timezone.utc)

    logger.info(f"Fetching up to {LIMIT_INSTRUMENTS} instruments (offset: {OFFSET_INSTRUMENTS})")

    # SELECT instruments list - you can adjust SQL to match your instruments table
    with engine.connect() as conn:
        df_candidates = pd.read_sql("""
            SELECT instrument_token, tradingsymbol, exchange
            FROM instruments
            WHERE exchange ILIKE 'NSE'  -- adjust as needed
            ORDER BY tradingsymbol
            LIMIT %s OFFSET %s
        """, conn, params=(LIMIT_INSTRUMENTS, OFFSET_INSTRUMENTS))

    logger.info(f"Found {len(df_candidates)} instruments to process")
    if OFFSET_INSTRUMENTS > 0:
        logger.info(f"Processing batch: instruments {OFFSET_INSTRUMENTS + 1} to {OFFSET_INSTRUMENTS + len(df_candidates)}")

    summary = []
    total_tasks = len(df_candidates) * len(INTERVALS)
    pbar_overall = tqdm(total=total_tasks, desc="Overall Progress", position=0)

    try:
        for idx, r in df_candidates.iterrows():
            tkn = int(r['instrument_token'])
            sym = r['tradingsymbol']
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing: {sym} ({tkn}) - {idx+1}/{len(df_candidates)}")
            logger.info(f"{'='*60}")

            for itv in INTERVALS:
                lookback_years = INTERVAL_LOOKBACK_YEARS.get(itv, 3)
                start_date = END_DATE - timedelta(days=365 * lookback_years)
                chunk_days = CHUNK_DAYS_BY_INTERVAL.get(itv, 90)

                # Determine number of chunks accurately
                chunks = list(make_date_chunks(start_date, END_DATE, chunk_days, itv))
                total_chunks = len(chunks) if chunks else 1
                logger.info(f"{sym} - {itv}: {lookback_years} years ({total_chunks} chunks)")

                with tqdm(total=total_chunks, desc=f"  {sym} {itv}", position=1, leave=False) as pbar_interval:
                    res = process_instrument_interval(
                        instrument_token=tkn,
                        tradingsymbol=sym,
                        interval=itv,
                        start_ts=start_date,
                        end_ts=END_DATE,
                        chunk_days=chunk_days,
                        progress_bar=pbar_interval
                    )

                summary.append({
                    'symbol': sym,
                    'interval': itv,
                    'years': lookback_years,
                    'status': res['status'],
                    'inserted': res.get('inserted', 0),
                    'chunks': res.get('chunks', 0),
                    'failed': res.get('failed', 0)
                })

                pbar_overall.update(1)
                # slight pause between intervals to avoid bursty requests across intervals
                time.sleep(1.0)

        # final summary
        df_summary = pd.DataFrame(summary)
        total_inserted = int(df_summary['inserted'].sum()) if not df_summary.empty else 0
        logger.info("\n" + "=" * 80)
        logger.info("INGESTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Batch offset: {OFFSET_INSTRUMENTS} instruments")
        logger.info(f"Total instruments processed: {len(df_candidates)}")
        logger.info(f"Total intervals: {len(INTERVALS)}")
        logger.info(f"Total rows inserted (approx): {total_inserted:,}")
        logger.info(f"Successful jobs: {len(df_summary[df_summary['status'] == 'done'])}")
        logger.info(f"Failed jobs: {len(df_summary[df_summary['status'] == 'error'])}")

        # Save summary CSV
        summary_file = f"ingestion_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df_summary.to_csv(summary_file, index=False)
        logger.info(f"Detailed summary saved to: {summary_file}")

        # Top performers
        if not df_summary.empty:
            logger.info("\nTop 10 by rows inserted:")
            top10 = df_summary.nlargest(10, 'inserted')[['symbol', 'interval', 'inserted']]
            logger.info("\n" + top10.to_string(index=False))

        logger.info("\n" + "=" * 80)
        logger.info("Ingestion Complete!")
        logger.info("=" * 80)
    finally:
        pbar_overall.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Ingestion interrupted by user")
        sys.exit(1)
    except Exception:
        logger.exception("Fatal error in ingestion")
        sys.exit(1)
