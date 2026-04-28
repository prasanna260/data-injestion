#!/usr/bin/env python3
"""
Daily 15-minute OHLCV data ingestion for INDICES
Fetches 15-minute candles for all instruments with segment='INDICES'
Usage: python ingest_indices_15min.py [YYYY-MM-DD]
If no date provided, uses today's date
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone, time as dt_time
from io import StringIO
import pandas as pd
import psycopg2
from kiteconnect import KiteConnect
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from tqdm import tqdm
import pytz

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ingest_indices_15min.log'),
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

# Timezone
IST = pytz.timezone('Asia/Kolkata')
UTC = pytz.UTC


def get_market_hours(target_date=None):
    """Get market hours for a specific date in IST (9:15 AM to 3:30 PM)
    
    Args:
        target_date: datetime.date object or None (uses today if None)
    
    Returns:
        tuple: (market_start, market_end) as IST datetime objects
    """
    now_ist = datetime.now(IST)
    
    # Use provided date or today
    if target_date is None:
        target_date = now_ist.date()
    
    # Market start: 9:15 AM IST
    market_start = IST.localize(datetime.combine(target_date, dt_time(9, 15, 0)))
    
    # Market end: 3:30 PM IST
    market_end = IST.localize(datetime.combine(target_date, dt_time(15, 30, 0)))
    
    # If fetching today's data and market is still open, use current time
    if target_date == now_ist.date() and now_ist < market_end:
        # Use current time minus 1 minute to ensure candle is complete
        market_end = now_ist - timedelta(minutes=1)
    
    # Return IST times (Kite API expects IST)
    return market_start, market_end


def fetch_historical_15min(instrument_token, tradingsymbol, from_date, to_date):
    """Fetch 15-minute historical data from Kite Connect API
    Note: Kite API expects IST times, not UTC
    """
    try:
        logger.debug(f"  Fetching {tradingsymbol}: {from_date.strftime('%Y-%m-%d %H:%M:%S %Z')} to {to_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        
        time.sleep(0.25)  # Rate limiting
        
        records = kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_date,
            to_date=to_date,
            interval='15minute',
            continuous=False,
            oi=True
        )
        
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        if 'date' in df.columns:
            df = df.rename(columns={'date': 'ts'})
        
        logger.debug(f"  Received {len(df)} candles for {tradingsymbol}")
        if not df.empty:
            logger.debug(f"  Time range: {df['ts'].min()} to {df['ts'].max()}")
        
        return df
        
    except Exception as e:
        err_str = str(e)
        if '503' in err_str or 'rate' in err_str.lower() or 'too many' in err_str.lower():
            logger.warning(f"Rate limit/503 error: {e}")
            time.sleep(1.0)
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


def ingest_candles_df(df_candles, instrument_token, tradingsymbol, exchange='NSE'):
    """Normalize and bulk insert 15-minute candles using temp table approach"""
    df = df_candles.copy()
    if df.empty:
        return 0

    df['tradingsymbol'] = tradingsymbol
    df['instrument_token'] = int(instrument_token)
    df['exchange'] = exchange
    df['interval'] = '15minute'

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
            ON CONFLICT (instrument_token, interval, ts) 
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                oi = EXCLUDED.oi
        """)
        
        inserted = cur.rowcount
        conn.commit()
        return inserted
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def process_index(instrument_token, tradingsymbol, exchange, from_date, to_date, max_retries=3):
    """Process one index for today's 15-minute data - fetch in hourly chunks"""
    inserted_total = 0
    all_data = []
    
    # Split into hourly chunks to ensure we get all data
    current_start = from_date
    chunk_duration = timedelta(hours=1)
    
    chunks_to_fetch = []
    while current_start < to_date:
        current_end = min(current_start + chunk_duration, to_date)
        chunks_to_fetch.append((current_start, current_end))
        current_start = current_end
    
    logger.info(f"  {tradingsymbol}: Fetching {len(chunks_to_fetch)} hourly chunks from {from_date.strftime('%H:%M')} to {to_date.strftime('%H:%M')}")
    
    for chunk_start, chunk_end in chunks_to_fetch:
        for attempt in range(1, max_retries + 1):
            try:
                df_chunk = fetch_historical_15min(instrument_token, tradingsymbol, chunk_start, chunk_end)
                
                if not df_chunk.empty:
                    all_data.append(df_chunk)
                    logger.info(f"    Chunk {chunk_start.strftime('%H:%M')}-{chunk_end.strftime('%H:%M')}: Got {len(df_chunk)} candles")
                else:
                    logger.info(f"    Chunk {chunk_start.strftime('%H:%M')}-{chunk_end.strftime('%H:%M')}: No data")
                
                break  # Success, move to next chunk
                
            except Exception as e:
                logger.warning(f"  ✗ {tradingsymbol}: Chunk {chunk_start.strftime('%H:%M')} attempt {attempt}/{max_retries} failed: {str(e)[:100]}")
                if attempt < max_retries:
                    time.sleep(1.0 * attempt)
                else:
                    logger.error(f"  ❌ {tradingsymbol}: Failed chunk {chunk_start.strftime('%H:%M')} after {max_retries} attempts")
        
        time.sleep(0.3)  # Small delay between chunks
    
    # Combine all chunks
    if not all_data:
        logger.info(f"  {tradingsymbol}: No data available from any chunk")
        return 0
    
    df_combined = pd.concat(all_data, ignore_index=True)
    
    # Remove duplicates (in case of overlapping chunks)
    df_combined = df_combined.drop_duplicates(subset=['ts'], keep='last')
    df_combined = df_combined.sort_values('ts')
    
    logger.info(f"  {tradingsymbol}: Combined {len(df_combined)} unique candles from {len(all_data)} chunks")
    
    # Insert to database
    try:
        inserted = ingest_candles_df(df_combined, instrument_token, tradingsymbol, exchange)
        inserted_total += inserted
        
        if inserted > 0:
            logger.info(f"  ✓ {tradingsymbol}: Inserted/Updated {inserted} candles (Total fetched: {len(df_combined)})")
        else:
            logger.info(f"  {tradingsymbol}: {len(df_combined)} candles already up to date")
        
        return inserted_total
    except Exception as e:
        logger.error(f"  ❌ {tradingsymbol}: Database insert failed: {e}")
        return 0


def main():
    """Main orchestrator for daily 10-minute indices data ingestion"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Ingest 15-minute OHLCV data for indices',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ingest_indices_15min.py                    # Fetch today's data
  python ingest_indices_15min.py 2026-02-06         # Fetch data for Feb 6, 2026
  python ingest_indices_15min.py 2026-02-05         # Fetch yesterday's data
        """
    )
    parser.add_argument(
        'date',
        nargs='?',
        default=None,
        help='Date to fetch data for (YYYY-MM-DD format). Defaults to today.'
    )
    
    args = parser.parse_args()
    
    # Parse target date
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD format.")
            sys.exit(1)
    else:
        target_date = datetime.now(IST).date()
    
    logger.info("=" * 80)
    logger.info("Starting Daily 15-Minute OHLCV Ingestion for INDICES")
    logger.info("=" * 80)
    
    # Get market hours for target date
    market_start, market_end = get_market_hours(target_date)
    
    now_ist = datetime.now(IST)
    now_utc = datetime.now(UTC)
    logger.info(f"Current time (IST): {now_ist.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Current time (UTC): {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Target date: {target_date.strftime('%Y-%m-%d')} ({target_date.strftime('%A')})")
    logger.info(f"Fetching data from: {market_start.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"Fetching data to:   {market_end.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Warn if fetching future date
    if target_date > now_ist.date():
        logger.warning(f"⚠️  Target date {target_date} is in the future. No data will be available.")
    
    # Warn if fetching weekend
    if target_date.weekday() >= 5:  # Saturday=5, Sunday=6
        logger.warning(f"⚠️  Target date {target_date} is a weekend. Market may be closed.")
    
    # Get all INDICES from database
    query = """
        SELECT instrument_token, tradingsymbol, exchange
        FROM instruments
        WHERE segment = 'INDICES'
          AND is_active = true
        ORDER BY tradingsymbol
    """
    
    with engine.connect() as conn:
        df_indices = pd.read_sql(query, conn)
    
    if df_indices.empty:
        logger.error("No indices found in database with segment='INDICES'")
        return
    
    logger.info(f"Found {len(df_indices)} indices to process")
    logger.info(f"Indices: {', '.join(df_indices['tradingsymbol'].tolist())}")
    logger.info("")
    
    # Process each index
    summary = []
    total_inserted = 0
    
    with tqdm(total=len(df_indices), desc="Processing Indices") as pbar:
        for idx, row in df_indices.iterrows():
            token = int(row['instrument_token'])
            symbol = row['tradingsymbol']
            exchange = row['exchange']
            
            logger.info(f"[{idx+1}/{len(df_indices)}] Processing: {symbol}")
            
            inserted = process_index(
                instrument_token=token,
                tradingsymbol=symbol,
                exchange=exchange,
                from_date=market_start,
                to_date=market_end,
                max_retries=3
            )
            
            summary.append({
                'symbol': symbol,
                'exchange': exchange,
                'inserted': inserted
            })
            
            total_inserted += inserted
            pbar.update(1)
            
            # Rate limiting between indices
            time.sleep(0.5)
    
    # Summary report
    logger.info("\n" + "=" * 80)
    logger.info("INGESTION SUMMARY")
    logger.info("=" * 80)
    
    df_summary = pd.DataFrame(summary)
    
    logger.info(f"\nTarget date: {target_date.strftime('%Y-%m-%d')}")
    logger.info(f"Total indices processed: {len(df_indices)}")
    logger.info(f"Total 15-minute candles inserted/updated: {total_inserted:,}")
    logger.info(f"Successful: {len(df_summary[df_summary['inserted'] > 0])}")
    logger.info(f"No data/Failed: {len(df_summary[df_summary['inserted'] == 0])}")
    
    # Save summary to CSV
    summary_file = f"indices_15min_summary_{target_date.strftime('%Y%m%d')}_{datetime.now().strftime('%H%M%S')}.csv"
    df_summary.to_csv(summary_file, index=False)
    logger.info(f"\nDetailed summary saved to: {summary_file}")
    
    # Print top performers
    if not df_summary.empty and df_summary['inserted'].sum() > 0:
        logger.info("\nTop indices by candles inserted:")
        top = df_summary.nlargest(10, 'inserted')[['symbol', 'inserted']]
        logger.info("\n" + top.to_string(index=False))
    
    logger.info("\n" + "=" * 80)
    logger.info("15-Minute Indices Ingestion Complete!")
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
