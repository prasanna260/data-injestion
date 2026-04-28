#!/usr/bin/env python3
"""
Check Latest Data in Database
Quick script to verify your daily updates are working
"""

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

print("🔍 Checking Latest Data in Database")
print("=" * 50)

with engine.connect() as conn:
    # Check latest data by symbol
    print("\n📊 Latest data by symbol:")
    result = conn.execute(text("""
        SELECT tradingsymbol,
               COUNT(*) as total_rows,
               MIN(ts)::date as earliest_date,
               MAX(ts)::date as latest_date
        FROM ohlcv
        WHERE interval = '1day'
        GROUP BY tradingsymbol
        ORDER BY MAX(ts) DESC
        LIMIT 10
    """))

    for row in result:
        print("25")

    # Check total rows by interval
    print("\n📈 Total rows by interval:")
    result = conn.execute(text("""
        SELECT interval, COUNT(*) as total_rows
        FROM ohlcv
        GROUP BY interval
        ORDER BY interval
    """))

    for row in result:
        print("15")

    # Check recent ingestion jobs
    print("\n⏰ Recent ingestion jobs:")
    result = conn.execute(text("""
        SELECT tradingsymbol, interval, status, last_ingested_ts::date, updated_at::date
        FROM ingest_jobs
        WHERE updated_at >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY updated_at DESC
        LIMIT 10
    """))

    for row in result:
        print("25")

print("\n✅ Database check complete!")
