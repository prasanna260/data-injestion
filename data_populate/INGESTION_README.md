# Historical Data Ingestion Script

A robust Python script for ingesting historical OHLCV data from Kite Connect API into PostgreSQL with progress tracking, resume capability, and error handling.

## Features

- ✅ **Progress Tracking**: Real-time progress bars using `tqdm`
- ✅ **Resume Capability**: Automatically resumes from last successful chunk
- ✅ **Duplicate Handling**: Uses temp tables + ON CONFLICT for safe re-runs
- ✅ **Rate Limiting**: Built-in delays to respect API limits
- ✅ **Error Recovery**: Automatic retries with exponential backoff
- ✅ **Logging**: Detailed logs to both file and console
- ✅ **Summary Reports**: CSV export of ingestion results

## Installation

```bash
# Install dependencies
pip install -r requirements_ingestion.txt
```

## Configuration

Create a `.env` file with your credentials:

```env
# Database
RAILWAY_DB_URI=postgresql://user:pass@host:port/database

# Kite Connect API
KITE_API_KEY=your_api_key
KITE_API_SECRET=your_api_secret
KITE_ACCESS_TOKEN=your_access_token

# Optional: Limit number of instruments (default: 10)
LIMIT_INSTRUMENTS=10
```

## Usage

### Basic Usage

```bash
python data_ingestion.py
```

### With Custom Instrument Limit

```bash
LIMIT_INSTRUMENTS=50 python data_ingestion.py
```

### Run in Background

```bash
nohup python data_ingestion.py > ingestion.out 2>&1 &
```

## Data Retention Policy

The script fetches different amounts of historical data based on interval:

| Interval | Lookback Period |
|----------|----------------|
| 1 minute | 1 year |
| 3 minute | 2 years |
| 5 minute | 2 years |
| 15 minute | 3 years |
| 30 minute | 3 years |
| 60 minute | 7 years |
| 180 minute | 7 years |
| Daily | 20 years |
| Weekly | 20 years |
| Monthly | 20 years |

## Progress Tracking

The script shows:
- Overall progress across all instruments and intervals
- Per-interval progress with chunk-level details
- Real-time insertion counts
- Success/failure indicators

Example output:
```
Overall Progress: 45%|████████████          | 45/100 [12:34<15:23, 16.79s/it]
  5minute: 80%|████████████████  | 8/10 [00:45<00:11, 5.67s/it]
  ✓ Inserted 812 rows: 2024-01-01 -> 2024-01-11
```

## Logging

Logs are written to:
- **Console**: Real-time progress and important messages
- **data_ingestion.log**: Detailed log file with timestamps

## Output Files

- `data_ingestion.log`: Detailed execution log
- `ingestion_summary_YYYYMMDD_HHMMSS.csv`: Summary of all ingestion jobs

## Resume Capability

The script tracks progress in the `ingest_jobs` table. If interrupted:
1. Simply re-run the script
2. It will automatically resume from the last successful chunk
3. Already-ingested data is skipped (no duplicates)

## Error Handling

- **503 Errors**: Automatic retry with exponential backoff
- **Rate Limits**: Built-in delays between API calls
- **Network Issues**: Retries up to 5 times per chunk
- **Database Errors**: Transaction rollback and error logging

## Performance Tips

1. **Rate Limiting**: Adjust sleep times in the script if you hit rate limits
2. **Chunk Sizes**: Modify `CHUNK_DAYS_BY_INTERVAL` for different chunk sizes
3. **Parallel Processing**: Run multiple instances with different instrument ranges
4. **Database**: Ensure proper indexes exist (created by schema)

## Monitoring

Check progress:
```bash
# Watch log file
tail -f data_ingestion.log

# Check database
psql $DATABASE_URL -c "SELECT status, COUNT(*) FROM ingest_jobs GROUP BY status;"
```

## Troubleshooting

### "503 Service Unavailable"
- Increase sleep times between chunks
- Reduce chunk sizes
- Run during off-peak hours

### "Rate limit exceeded"
- Increase `sleep_between_chunks` parameter
- Add delay in `fetch_historical` function

### "Connection timeout"
- Check database connectivity
- Verify Kite API credentials
- Check network/firewall settings

### Resume from specific point
```sql
-- Reset a specific job to retry
UPDATE ingest_jobs 
SET status = 'pending', last_ingested_ts = NULL 
WHERE instrument_token = 3343617 AND interval = '1minute';
```

## Database Schema

The script expects these tables:
- `instruments`: Instrument master data
- `ohlcv`: OHLCV candle data
- `ingest_jobs`: Job tracking for resume capability

## License

MIT License - See LICENSE file for details
