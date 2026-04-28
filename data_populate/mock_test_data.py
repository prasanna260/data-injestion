import logging
import os
import sys
import uuid
import atexit
import signal
import random
import threading
import queue
from io import StringIO
from collections import defaultdict
from datetime import datetime, timedelta, time

from dotenv import load_dotenv
from kiteconnect import KiteTicker, KiteConnect
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from sqlalchemy import create_engine, Column, DateTime, BigInteger, Text, Numeric
from sqlalchemy.orm import declarative_base, sessionmaker
import pytz
import urllib.parse as urlparse
import time as time_module


# =========================================================
# ENV / CONFIG
# =========================================================
load_dotenv()

DEFAULT_UPDATE_INTERVAL = 60
MIN_UPDATE_INTERVAL = 15
DEFAULT_MOCK_TICK_INTERVAL = 1.0

IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.UTC

USE_MOCK_STREAM = os.getenv("USE_MOCK_STREAM", "false").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("streamer.log"),
        logging.StreamHandler()
    ]
)

# Optional cache import
try:
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    from cache_service import cache
    CACHE_ENABLED = True
    logging.info("✅ Cache service imported successfully")
except ImportError as e:
    logging.warning(f"⚠️ Cache service not available: {e}")
    CACHE_ENABLED = False
    cache = None


# =========================================================
# DB SETUP
# =========================================================
connection_string = os.getenv("DATABASE_URL")
if not connection_string:
    raise RuntimeError("DATABASE_URL not found in environment")

engine = create_engine(connection_string, pool_size=10, max_overflow=20)

url = urlparse.urlparse(connection_string)

db_pool = ThreadedConnectionPool(
    minconn=2,
    maxconn=10,
    host=url.hostname,
    database=url.path[1:],
    user=url.username,
    password=url.password,
    port=url.port
)

Base = declarative_base()
Session = sessionmaker(bind=engine)


class OHLCVData(Base):
    __tablename__ = "ohlcv"

    instrument_token = Column(BigInteger, nullable=False, primary_key=True)
    tradingsymbol = Column(Text)
    exchange = Column(Text)
    interval = Column(Text, nullable=False, primary_key=True)
    ts = Column(DateTime, nullable=False, primary_key=True)
    open = Column(Numeric)
    high = Column(Numeric)
    low = Column(Numeric)
    close = Column(Numeric)
    volume = Column(BigInteger)
    oi = Column(BigInteger)


# =========================================================
# KITE CREDS
# =========================================================
api_key = os.getenv("KITE_API_KEY")
api_secret = os.getenv("KITE_API_SECRET")
access_token_env = os.getenv("KITE_ACCESS_TOKEN")


# =========================================================
# MOCK KITE TICKER
# =========================================================
class MockKiteTicker:
    """
    Drop-in mock for KiteTicker with the same callback style.
    """

    MODE_FULL = "full"

    def __init__(self, api_key, access_token, instrument_lookup, tick_interval=1.0):
        self.api_key = api_key
        self.access_token = access_token
        self.instrument_lookup = instrument_lookup
        self.tick_interval = tick_interval

        self.on_ticks = None
        self.on_connect = None
        self.on_close = None
        self.on_error = None

        self._subscribed_tokens = []
        self._running = False
        self._mode = self.MODE_FULL
        self._state = {}

    def subscribe(self, tokens):
        self._subscribed_tokens = list(tokens)
        for token in self._subscribed_tokens:
            if token not in self._state:
                self._state[token] = self._init_instrument_state(token)

    def set_mode(self, mode, tokens):
        self._mode = mode

    def stop(self):
        self._running = False

    def _init_instrument_state(self, token):
        info = self.instrument_lookup.get(token, {})
        symbol = info.get("tradingsymbol", f"TOKEN_{token}")
        segment = info.get("segment", "")

        local_rng = random.Random(int(token) % 100000)

        if segment == "INDICES":
            base_price = local_rng.uniform(15000, 55000)
            volume = 0
            oi = 0
        else:
            base_price = local_rng.uniform(50, 3000)
            volume = local_rng.randint(1000, 20000)
            oi = local_rng.randint(1000, 100000)

        last_close = round(base_price * local_rng.uniform(0.985, 1.015), 2)

        return {
            "symbol": symbol,
            "segment": segment,
            "price": round(base_price, 2),
            "last_close": last_close,
            "day_high": round(base_price, 2),
            "day_low": round(base_price, 2),
            "volume_traded": volume,
            "oi": oi,
        }

    def _generate_tick(self, token):
        state = self._state[token]
        segment = state["segment"]
        old_price = state["price"]

        if segment == "INDICES":
            pct_move = random.uniform(-0.0008, 0.0008)
        else:
            pct_move = random.uniform(-0.0025, 0.0025)

        new_price = round(max(0.05, old_price * (1 + pct_move)), 2)

        state["price"] = new_price
        state["day_high"] = max(state["day_high"], new_price)
        state["day_low"] = min(state["day_low"], new_price)

        if segment != "INDICES":
            state["volume_traded"] += random.randint(1, 500)
            state["oi"] = max(0, state["oi"] + random.randint(-100, 100))
        else:
            state["volume_traded"] = 0
            state["oi"] = 0

        change = 0.0
        if state["last_close"]:
            change = ((new_price - state["last_close"]) / state["last_close"]) * 100

        now_ist = datetime.now(IST)

        tick = {
            "tradable": True,
            "mode": "full",
            "instrument_token": token,
            "last_price": new_price,
            "last_quantity": random.randint(1, 100) if segment != "INDICES" else 0,
            "average_traded_price": new_price,
            "volume_traded": state["volume_traded"],
            "total_buy_quantity": random.randint(100, 10000),
            "total_sell_quantity": random.randint(100, 10000),
            "ohlc": {
                "open": state["last_close"],
                "high": state["day_high"],
                "low": state["day_low"],
                "close": state["last_close"],
            },
            "change": round(change, 4),
            "last_trade_time": now_ist,
            "timestamp": now_ist,
            "oi": state["oi"],
            "oi_day_high": state["oi"] + random.randint(0, 1000) if segment != "INDICES" else 0,
            "oi_day_low": max(0, state["oi"] - random.randint(0, 1000)) if segment != "INDICES" else 0,
            "depth": {
                "buy": [],
                "sell": []
            }
        }
        return tick

    def _run_loop(self):
        try:
            if self.on_connect:
                self.on_connect(self, None)

            self._running = True
            logging.info("🧪 MockKiteTicker started generating ticks...")

            while self._running:
                ticks = [self._generate_tick(token) for token in self._subscribed_tokens]

                if ticks and self.on_ticks:
                    self.on_ticks(self, ticks)

                time_module.sleep(self.tick_interval)

        except Exception as e:
            if self.on_error:
                self.on_error(self, 500, str(e))
        finally:
            if self.on_close:
                self.on_close(self, 1000, "Mock stream stopped")

    def connect(self, threaded=False):
        if threaded:
            t = threading.Thread(target=self._run_loop, daemon=True)
            t.start()
        else:
            self._run_loop()


# =========================================================
# STREAMER
# =========================================================
class RealTimeStreamer:
    def __init__(self, update_interval=None, use_mock=False, mock_tick_interval=1.0):
        logging.info("🔧 Initializing RealTimeStreamer...")

        self.session = Session()
        self.instruments_data = {}
        self.tick_buffer = defaultdict(list)

        self.processing_queue = queue.Queue(maxsize=5000)
        self.db_queue = queue.Queue(maxsize=1000)

        self.processing_worker_thread = None
        self.processing_worker_running = False
        self.db_worker_threads = []
        self.db_worker_running = False
        self.num_db_workers = 1

        self.last_hour_update = None
        self.last_day_update = None
        self.last_status_update = None
        self.last_interval_update = None

        self.total_ticks_received = 0
        self.total_ticks_processed = 0
        self.total_records_processed = 0
        self.total_db_operations = 0
        self.failed_db_operations = 0

        self.kws = None
        self.access_token = None

        self.use_mock = use_mock
        self.mock_tick_interval = mock_tick_interval

        self.ohlc_lock = threading.Lock()
        self.hourly_ohlc = defaultdict(lambda: {
            "open": None, "high": None, "low": None, "close": None,
            "volume": 0, "volume_start": None, "oi": 0,
            "first_tick_time": None, "last_tick_time": None
        })
        self.daily_ohlc = defaultdict(lambda: {
            "open": None, "high": None, "low": None, "close": None,
            "volume": 0, "volume_start": None, "oi": 0,
            "first_tick_time": None, "last_tick_time": None
        })
        self.fifteen_min_ohlc = defaultdict(lambda: {
            "open": None, "high": None, "low": None, "close": None,
            "volume": 0, "volume_start": None, "oi": 0,
            "first_tick_time": None, "last_tick_time": None
        })

        self.update_interval = self._get_update_interval(update_interval)

        # Mock run tracking
        self.mock_mode = self.use_mock
        self.mock_run_id = os.getenv("MOCK_RUN_ID") or str(uuid.uuid4())
        self.mock_run_id_file = "/tmp/mock_streamer_last_run_id.txt"
        self.cleanup_done = False

        logging.info("📥 Loading instruments from database...")
        self.load_instruments()
        logging.info(f"✅ Loaded {len(self.instruments_data)} instruments")

        if self.use_mock:
            self._ensure_mock_recovery_tables()
            with open(self.mock_run_id_file, "w") as f:
                f.write(self.mock_run_id)
            logging.info(f"🧪 Mock run id: {self.mock_run_id}")
        else:
            self.setup_kite_connection()

        self.start_processing_worker()
        self.start_db_worker()
        logging.info("✅ RealTimeStreamer initialization complete")

    # -----------------------------------------------------
    # CONFIG / SETUP
    # -----------------------------------------------------
    def _get_update_interval(self, interval):
        if interval is not None:
            configured_interval = interval
        else:
            env_interval = os.getenv("STREAMER_UPDATE_INTERVAL")
            if env_interval:
                try:
                    configured_interval = int(env_interval)
                except ValueError:
                    logging.warning(f"Invalid STREAMER_UPDATE_INTERVAL value: {env_interval}. Using default.")
                    configured_interval = DEFAULT_UPDATE_INTERVAL
            else:
                configured_interval = DEFAULT_UPDATE_INTERVAL

        if configured_interval < MIN_UPDATE_INTERVAL:
            logging.warning(
                f"Update interval {configured_interval}s is too low. Minimum is {MIN_UPDATE_INTERVAL}s. Using minimum."
            )
            configured_interval = MIN_UPDATE_INTERVAL

        logging.info(f"📊 Update interval configured: {configured_interval} seconds")
        return configured_interval

    def load_instruments(self):
        conn = None
        cursor = None
        try:
            logging.info("🔌 Getting database connection from pool...")
            conn = db_pool.getconn()
            cursor = conn.cursor()

            logging.info("📊 Querying instruments table...")
            cursor.execute("""
                SELECT instrument_token, tradingsymbol, exchange, segment
                FROM instruments
                WHERE is_active = true
                ORDER BY tradingsymbol
            """)

            instruments = cursor.fetchall()
            logging.info(f"✅ Fetched {len(instruments)} instruments from database")

            cache_available = CACHE_ENABLED and cache and cache.is_available()
            if cache_available:
                logging.info("✅ Cache is available")
            else:
                logging.info("⚠️ Cache is not available - skipping cache population")

            for token, symbol, exchange, segment in instruments:
                self.instruments_data[token] = {
                    "tradingsymbol": symbol,
                    "exchange": exchange,
                    "segment": segment
                }

            logging.info(f"✅ Loaded {len(self.instruments_data)} instruments into memory")

            if cache_available:
                try:
                    cache.set_instrument_batch(instruments)
                    logging.info(f"✅ Populated cache with {len(instruments)} instrument mappings")
                except Exception as cache_error:
                    logging.warning(f"⚠️ Cache batch write failed: {cache_error}")

            logging.info("📋 Sample instruments:")
            for token, data in list(self.instruments_data.items())[:10]:
                logging.info(f"  {token}: {data['tradingsymbol']} ({data['exchange']}) [{data['segment']}]")

        except Exception as e:
            logging.error(f"❌ Error loading instruments: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)

    def setup_kite_connection(self):
        try:
            if access_token_env:
                self.access_token = access_token_env
                logging.info("✅ Loaded access token from environment")
            else:
                if not api_key or not api_secret:
                    raise RuntimeError("KITE_API_KEY / KITE_API_SECRET not configured")

                kite = KiteConnect(api_key=api_key)
                print("Login URL:", kite.login_url())
                request_token = input("Enter request token from login URL: ")

                session_data = kite.generate_session(
                    request_token=request_token,
                    api_secret=api_secret
                )
                self.access_token = session_data["access_token"]
                logging.info("✅ Obtained access token via manual login")
        except Exception as e:
            logging.error(f"❌ Error setting up Kite connection: {e}")
            raise

    # -----------------------------------------------------
    # MOCK RECOVERY TABLES / BACKUP / RESTORE
    # -----------------------------------------------------
    def _ensure_mock_recovery_tables(self):
        conn = None
        cursor = None
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ohlcv_mock_backup (
                    run_id TEXT NOT NULL,
                    instrument_token BIGINT NOT NULL,
                    interval TEXT NOT NULL,
                    ts TIMESTAMPTZ NOT NULL,
                    existed_before BOOLEAN NOT NULL,
                    tradingsymbol TEXT,
                    exchange TEXT,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume BIGINT,
                    oi BIGINT,
                    PRIMARY KEY (run_id, instrument_token, interval, ts)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ohlcv_mock_written_keys (
                    run_id TEXT NOT NULL,
                    instrument_token BIGINT NOT NULL,
                    interval TEXT NOT NULL,
                    ts TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (run_id, instrument_token, interval, ts)
                )
            """)

            conn.commit()
            logging.info("✅ Mock recovery tables ensured")
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"❌ Error creating mock recovery tables: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)

    def _backup_existing_rows_for_mock_run(self, cursor):
        if not self.use_mock:
            return

        cursor.execute("""
            INSERT INTO ohlcv_mock_backup (
                run_id, instrument_token, interval, ts, existed_before,
                tradingsymbol, exchange, open, high, low, close, volume, oi
            )
            SELECT
                %s,
                t.instrument_token,
                t.interval,
                t.ts,
                CASE WHEN o.instrument_token IS NULL THEN FALSE ELSE TRUE END,
                o.tradingsymbol,
                o.exchange,
                o.open,
                o.high,
                o.low,
                o.close,
                o.volume,
                o.oi
            FROM (
                SELECT DISTINCT instrument_token, interval, ts
                FROM temp_ohlcv_insert
            ) t
            LEFT JOIN ohlcv o
              ON o.instrument_token = t.instrument_token
             AND o.interval = t.interval
             AND o.ts = t.ts
            LEFT JOIN ohlcv_mock_backup b
              ON b.run_id = %s
             AND b.instrument_token = t.instrument_token
             AND b.interval = t.interval
             AND b.ts = t.ts
            WHERE b.run_id IS NULL
        """, (self.mock_run_id, self.mock_run_id))

        cursor.execute("""
            INSERT INTO ohlcv_mock_written_keys (run_id, instrument_token, interval, ts)
            SELECT DISTINCT %s, instrument_token, interval, ts
            FROM temp_ohlcv_insert
            ON CONFLICT DO NOTHING
        """, (self.mock_run_id,))

    def restore_mock_run_data(self):
        if not self.use_mock:
            return
        if self.cleanup_done:
            return

        conn = None
        cursor = None
        try:
            logging.info(f"♻️ Restoring DB state for mock run {self.mock_run_id} ...")
            conn = db_pool.getconn()
            cursor = conn.cursor()

            cursor.execute("""
                DELETE FROM ohlcv o
                USING ohlcv_mock_written_keys w
                WHERE w.run_id = %s
                  AND o.instrument_token = w.instrument_token
                  AND o.interval = w.interval
                  AND o.ts = w.ts
            """, (self.mock_run_id,))
            deleted_count = cursor.rowcount

            cursor.execute("""
                INSERT INTO ohlcv (
                    instrument_token, tradingsymbol, exchange, interval, ts,
                    open, high, low, close, volume, oi
                )
                SELECT
                    instrument_token, tradingsymbol, exchange, interval, ts,
                    open, high, low, close, volume, oi
                FROM ohlcv_mock_backup
                WHERE run_id = %s
                  AND existed_before = TRUE
                ON CONFLICT (instrument_token, interval, ts)
                DO UPDATE SET
                    tradingsymbol = EXCLUDED.tradingsymbol,
                    exchange = EXCLUDED.exchange,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    oi = EXCLUDED.oi
            """, (self.mock_run_id,))
            restored_count = cursor.rowcount

            cursor.execute("DELETE FROM ohlcv_mock_written_keys WHERE run_id = %s", (self.mock_run_id,))
            cursor.execute("DELETE FROM ohlcv_mock_backup WHERE run_id = %s", (self.mock_run_id,))

            conn.commit()
            self.cleanup_done = True

            logging.info(
                f"✅ Mock cleanup complete | deleted mock keys: {deleted_count}, restored originals: {restored_count}"
            )
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"❌ Failed restoring mock data for run {self.mock_run_id}: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)

    # -----------------------------------------------------
    # WORKERS
    # -----------------------------------------------------
    def start_processing_worker(self):
        self.processing_worker_running = True
        self.processing_worker_thread = threading.Thread(target=self._processing_worker, daemon=True)
        self.processing_worker_thread.start()
        logging.info("🔧 Processing worker thread started")

    def _processing_worker(self):
        logging.info("⚙️ Processing worker running...")
        while True:
            try:
                work_item = self.processing_queue.get(timeout=1.0)
            except queue.Empty:
                if not self.processing_worker_running:
                    break
                continue

            try:
                if work_item is None:
                    break

                ticks_batch, timestamp_ist = work_item
                ohlcv_records = self._aggregate_ticks_to_intervals_fast(ticks_batch, timestamp_ist)

                if ohlcv_records:
                    try:
                        self.db_queue.put_nowait(ohlcv_records)
                    except queue.Full:
                        logging.warning(f"⚠️ DB queue full! Dropping {len(ohlcv_records)} records")
                        self.failed_db_operations += 1

                self.total_ticks_processed += len(ticks_batch)
            except Exception as e:
                logging.error(f"Error in processing worker: {e}")
            finally:
                self.processing_queue.task_done()

        logging.info("⚙️ Processing worker stopped")

    def start_db_worker(self):
        self.db_worker_running = True
        for i in range(self.num_db_workers):
            thread = threading.Thread(target=self._db_worker, daemon=True, name=f"DBWorker-{i+1}")
            thread.start()
            self.db_worker_threads.append(thread)
        logging.info(f"🔧 Started {self.num_db_workers} database worker threads")

    def _db_worker(self):
        conn = None
        cursor = None
        try:
            conn = db_pool.getconn()
            cursor = conn.cursor()

            while True:
                try:
                    work_item = self.db_queue.get(timeout=1.0)
                except queue.Empty:
                    if not self.db_worker_running:
                        break
                    continue

                try:
                    if work_item is None:
                        break

                    ohlcv_records = work_item

                    if len(ohlcv_records) > 1000:
                        for i in range(0, len(ohlcv_records), 500):
                            chunk = ohlcv_records[i:i + 500]
                            try:
                                self._execute_bulk_upsert_fast(cursor, conn, chunk)
                            except psycopg2.extensions.TransactionRollbackError:
                                logging.warning("⚠️ Skipping chunk after deadlock retries exhausted")
                                self.failed_db_operations += 1
                            except Exception as e:
                                logging.error(f"Error processing chunk: {e}")
                                self.failed_db_operations += 1
                                try:
                                    conn.rollback()
                                except Exception:
                                    pass
                    else:
                        try:
                            self._execute_bulk_upsert_fast(cursor, conn, ohlcv_records)
                        except psycopg2.extensions.TransactionRollbackError:
                            logging.warning("⚠️ Skipping batch after deadlock retries exhausted")
                            self.failed_db_operations += 1
                        except Exception as e:
                            logging.error(f"Error processing batch: {e}")
                            self.failed_db_operations += 1
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                except Exception as e:
                    logging.error(f"Error in DB worker: {e}")
                    self.failed_db_operations += 1
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                finally:
                    self.db_queue.task_done()
        except Exception as e:
            logging.error(f"Fatal error in DB worker: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                db_pool.putconn(conn)
            logging.info("🔧 Database worker thread stopped")

    # -----------------------------------------------------
    # DB UPSERT
    # -----------------------------------------------------
    def _execute_bulk_upsert_fast(self, cursor, conn, ohlcv_records, retry_count=0, max_retries=3):
        if not ohlcv_records:
            return

        start_time = time_module.time()

        try:
            cursor.execute("""
                CREATE TEMP TABLE temp_ohlcv_insert (
                    instrument_token BIGINT,
                    tradingsymbol TEXT,
                    exchange TEXT,
                    interval TEXT,
                    ts TIMESTAMPTZ,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume BIGINT,
                    oi BIGINT
                ) ON COMMIT DROP
            """)

            buffer = StringIO()
            null_marker = "\\N"

            for record in ohlcv_records:
                buffer.write(f"{record['instrument_token']}\t")
                buffer.write(f"{record['tradingsymbol']}\t")
                buffer.write(f"{record['exchange']}\t")
                buffer.write(f"{record['interval']}\t")
                buffer.write(f"{record['ts']}\t")
                buffer.write(f"{record['open']}\t")
                buffer.write(f"{record['high']}\t")
                buffer.write(f"{record['low']}\t")
                buffer.write(f"{record['close']}\t")
                buffer.write(f"{record['volume']}\t")
                oi_value = record["oi"] if record["oi"] is not None else null_marker
                buffer.write(f"{oi_value}\n")

            buffer.seek(0)

            cursor.copy_from(buffer, "temp_ohlcv_insert", columns=[
                "instrument_token", "tradingsymbol", "exchange", "interval", "ts",
                "open", "high", "low", "close", "volume", "oi"
            ])

            if self.use_mock:
                self._backup_existing_rows_for_mock_run(cursor)

            cursor.execute("""
                INSERT INTO ohlcv (
                    instrument_token, tradingsymbol, exchange, interval, ts,
                    open, high, low, close, volume, oi
                )
                SELECT
                    instrument_token, tradingsymbol, exchange, interval, ts,
                    open, high, low, close, volume, oi
                FROM temp_ohlcv_insert
                ON CONFLICT (instrument_token, interval, ts)
                DO UPDATE SET
                    tradingsymbol = EXCLUDED.tradingsymbol,
                    exchange = EXCLUDED.exchange,
                    open = COALESCE(ohlcv.open, EXCLUDED.open),
                    high = GREATEST(COALESCE(ohlcv.high, 0), COALESCE(EXCLUDED.high, 0)),
                    low = CASE
                        WHEN ohlcv.low IS NULL THEN EXCLUDED.low
                        WHEN EXCLUDED.low IS NULL THEN ohlcv.low
                        ELSE LEAST(ohlcv.low, EXCLUDED.low)
                    END,
                    close = EXCLUDED.close,
                    volume = GREATEST(COALESCE(ohlcv.volume, 0), COALESCE(EXCLUDED.volume, 0)),
                    oi = EXCLUDED.oi
            """)

            conn.commit()

            if CACHE_ENABLED and cache and cache.is_available():
                try:
                    cache_safe_records = []
                    for record in ohlcv_records:
                        safe_record = dict(record)
                        if isinstance(safe_record.get("ts"), datetime):
                            safe_record["ts"] = safe_record["ts"].isoformat()
                        cache_safe_records.append(safe_record)

                    cache.set_ohlc_batch(cache_safe_records, ttl=3600)
                except Exception as cache_error:
                    logging.warning(f"⚠️ Cache write failed: {cache_error}")

            elapsed = time_module.time() - start_time
            self.total_db_operations += 1
            self.total_records_processed += len(ohlcv_records)

            fifteen_min_records = sum(1 for r in ohlcv_records if r["interval"] == "15minute")
            hourly_records = sum(1 for r in ohlcv_records if r["interval"] == "60minute")
            daily_records = sum(1 for r in ohlcv_records if r["interval"] == "1day")

            logging.info(
                f"💾 Saved {len(ohlcv_records)} records in {elapsed:.2f}s "
                f"({len(ohlcv_records) / elapsed:.0f} rec/s) | "
                f"{fifteen_min_records} 15min, {hourly_records} hourly, {daily_records} daily | "
                f"Queue: {self.db_queue.qsize()}"
            )

        except psycopg2.extensions.TransactionRollbackError:
            conn.rollback()
            if retry_count < max_retries:
                wait_time = 0.1 * (2 ** retry_count)
                logging.warning(
                    f"⚠️ Deadlock detected, retrying in {wait_time}s (attempt {retry_count + 1}/{max_retries})"
                )
                time_module.sleep(wait_time)
                return self._execute_bulk_upsert_fast(cursor, conn, ohlcv_records, retry_count + 1, max_retries)
            logging.error(f"❌ Deadlock persisted after {max_retries} retries, dropping {len(ohlcv_records)} records")
            raise
        except Exception as e:
            logging.error(f"Error in bulk upsert: {e}")
            conn.rollback()
            raise

    # -----------------------------------------------------
    # TIME HELPERS
    # -----------------------------------------------------
    def is_market_hours(self):
        ist_now = datetime.now(IST)
        current_time = ist_now.time()
        market_start = time(9, 15)
        market_end = time(15, 30)
        return market_start <= current_time <= market_end

    def get_utc_timestamp(self, local_dt=None):
        if local_dt is None:
            ist_now = datetime.now(IST)
            return ist_now.astimezone(UTC)
        if local_dt.tzinfo is None:
            ist_dt = IST.localize(local_dt)
        else:
            ist_dt = local_dt
        return ist_dt.astimezone(UTC)

    # -----------------------------------------------------
    # OHLC AGGREGATION
    # -----------------------------------------------------
    def _update_ohlc_data_fast(self, ticks, timestamp_utc):
        with self.ohlc_lock:
            for tick in ticks:
                instrument_token = tick["instrument_token"]
                if instrument_token not in self.instruments_data:
                    continue

                instrument_info = self.instruments_data[instrument_token]
                is_index = instrument_info.get("segment") == "INDICES"

                last_price = tick.get("last_price")
                volume = tick.get("volume_traded", 0) if not is_index else 0
                oi = tick.get("oi", 0)

                if last_price is None:
                    continue

                hour_key = (instrument_token, timestamp_utc.replace(minute=0, second=0, microsecond=0))
                day_key = (instrument_token, timestamp_utc.replace(hour=0, minute=0, second=0, microsecond=0))

                if is_index:
                    minute = (timestamp_utc.minute // 15) * 15
                    fifteen_min_ts = timestamp_utc.replace(minute=minute, second=0, microsecond=0)
                    fifteen_min_key = (instrument_token, fifteen_min_ts)

                    fifteen_min_data = self.fifteen_min_ohlc[fifteen_min_key]
                    if fifteen_min_data["open"] is None:
                        fifteen_min_data["open"] = last_price
                        fifteen_min_data["first_tick_time"] = timestamp_utc
                        fifteen_min_data["volume_start"] = volume

                    if fifteen_min_data["high"] is None or last_price > fifteen_min_data["high"]:
                        fifteen_min_data["high"] = last_price
                    if fifteen_min_data["low"] is None or last_price < fifteen_min_data["low"]:
                        fifteen_min_data["low"] = last_price

                    fifteen_min_data["close"] = last_price
                    fifteen_min_data["last_tick_time"] = timestamp_utc
                    if fifteen_min_data["volume_start"] is not None:
                        fifteen_min_data["volume"] = max(0, volume - fifteen_min_data["volume_start"])
                    else:
                        fifteen_min_data["volume"] = 0
                    fifteen_min_data["oi"] = oi

                hourly_data = self.hourly_ohlc[hour_key]
                if hourly_data["open"] is None:
                    hourly_data["open"] = last_price
                    hourly_data["first_tick_time"] = timestamp_utc
                    hourly_data["volume_start"] = volume

                if hourly_data["high"] is None or last_price > hourly_data["high"]:
                    hourly_data["high"] = last_price
                if hourly_data["low"] is None or last_price < hourly_data["low"]:
                    hourly_data["low"] = last_price

                hourly_data["close"] = last_price
                hourly_data["last_tick_time"] = timestamp_utc
                if hourly_data["volume_start"] is not None:
                    hourly_data["volume"] = max(0, volume - hourly_data["volume_start"])
                else:
                    hourly_data["volume"] = 0
                hourly_data["oi"] = oi

                daily_data = self.daily_ohlc[day_key]
                if daily_data["open"] is None:
                    daily_data["open"] = last_price
                    daily_data["first_tick_time"] = timestamp_utc
                    daily_data["volume_start"] = 0

                if daily_data["high"] is None or last_price > daily_data["high"]:
                    daily_data["high"] = last_price
                if daily_data["low"] is None or last_price < daily_data["low"]:
                    daily_data["low"] = last_price

                daily_data["close"] = last_price
                daily_data["last_tick_time"] = timestamp_utc
                daily_data["volume"] = volume
                daily_data["oi"] = oi

    def _aggregate_ticks_to_intervals_fast(self, ticks, timestamp):
        if not ticks:
            return []

        utc_timestamp = self.get_utc_timestamp(timestamp)
        self._update_ohlc_data_fast(ticks, utc_timestamp)

        ohlcv_records = []
        current_instruments = set(
            tick["instrument_token"]
            for tick in ticks
            if tick["instrument_token"] in self.instruments_data
        )

        with self.ohlc_lock:
            for instrument_token in current_instruments:
                instrument_info = self.instruments_data[instrument_token]
                is_index = instrument_info.get("segment") == "INDICES"

                if is_index:
                    minute = (utc_timestamp.minute // 15) * 15
                    fifteen_min_ts_utc = utc_timestamp.replace(minute=minute, second=0, microsecond=0)
                    fifteen_min_key = (instrument_token, fifteen_min_ts_utc)

                    if fifteen_min_key in self.fifteen_min_ohlc:
                        d = self.fifteen_min_ohlc[fifteen_min_key]
                        if d["open"] is not None:
                            ohlcv_records.append({
                                "instrument_token": instrument_token,
                                "tradingsymbol": instrument_info["tradingsymbol"],
                                "exchange": instrument_info["exchange"],
                                "interval": "15minute",
                                "ts": fifteen_min_ts_utc,
                                "open": d["open"],
                                "high": d["high"],
                                "low": d["low"],
                                "close": d["close"],
                                "volume": d["volume"],
                                "oi": d["oi"],
                            })

                hour_ts_utc = utc_timestamp.replace(minute=0, second=0, microsecond=0)
                hour_key = (instrument_token, hour_ts_utc)
                if hour_key in self.hourly_ohlc:
                    d = self.hourly_ohlc[hour_key]
                    if d["open"] is not None:
                        ohlcv_records.append({
                            "instrument_token": instrument_token,
                            "tradingsymbol": instrument_info["tradingsymbol"],
                            "exchange": instrument_info["exchange"],
                            "interval": "60minute",
                            "ts": hour_ts_utc,
                            "open": d["open"],
                            "high": d["high"],
                            "low": d["low"],
                            "close": d["close"],
                            "volume": d["volume"],
                            "oi": d["oi"],
                        })

                day_ts_utc = utc_timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
                day_key = (instrument_token, day_ts_utc)
                if day_key in self.daily_ohlc:
                    d = self.daily_ohlc[day_key]
                    if d["open"] is not None:
                        ohlcv_records.append({
                            "instrument_token": instrument_token,
                            "tradingsymbol": instrument_info["tradingsymbol"],
                            "exchange": instrument_info["exchange"],
                            "interval": "1day",
                            "ts": day_ts_utc,
                            "open": d["open"],
                            "high": d["high"],
                            "low": d["low"],
                            "close": d["close"],
                            "volume": d["volume"],
                            "oi": d["oi"],
                        })

        return ohlcv_records

    def cleanup_old_ohlc_tracking(self):
        with self.ohlc_lock:
            current_time_utc = datetime.now(UTC)

            fifteen_min_cutoff = current_time_utc - timedelta(hours=2)
            hourly_cutoff = current_time_utc - timedelta(hours=2)
            daily_cutoff = current_time_utc - timedelta(days=2)

            keys_to_remove_15min = []
            for key in self.fifteen_min_ohlc:
                key_timestamp = key[1]
                if key_timestamp.tzinfo is None:
                    key_timestamp = UTC.localize(key_timestamp)
                if key_timestamp < fifteen_min_cutoff:
                    keys_to_remove_15min.append(key)

            for key in keys_to_remove_15min:
                del self.fifteen_min_ohlc[key]

            keys_to_remove = []
            for key in self.hourly_ohlc:
                key_timestamp = key[1]
                if key_timestamp.tzinfo is None:
                    key_timestamp = UTC.localize(key_timestamp)
                if key_timestamp < hourly_cutoff:
                    keys_to_remove.append(key)

            for key in keys_to_remove:
                del self.hourly_ohlc[key]

            keys_to_remove_daily = []
            for key in self.daily_ohlc:
                key_timestamp = key[1]
                if key_timestamp.tzinfo is None:
                    key_timestamp = UTC.localize(key_timestamp)
                if key_timestamp < daily_cutoff:
                    keys_to_remove_daily.append(key)

            for key in keys_to_remove_daily:
                del self.daily_ohlc[key]

            total_removed = len(keys_to_remove_15min) + len(keys_to_remove) + len(keys_to_remove_daily)
            if total_removed > 0:
                logging.info(
                    f"🧹 Cleaned up {total_removed} old OHLC tracking entries "
                    f"({len(keys_to_remove_15min)} 15min, {len(keys_to_remove)} hourly, {len(keys_to_remove_daily)} daily)"
                )

    # -----------------------------------------------------
    # PIPELINE FLOW
    # -----------------------------------------------------
    def save_ohlcv_data_bulk(self, ohlcv_records):
        if not ohlcv_records:
            return
        try:
            self.db_queue.put_nowait(ohlcv_records)
            logging.debug(f"📤 Queued {len(ohlcv_records)} records for DB save (queue size: {self.db_queue.qsize()})")
        except queue.Full:
            logging.warning(f"⚠️ DB queue full! Dropping {len(ohlcv_records)} records.")
            self.failed_db_operations += 1

    def process_and_save_interval_data(self):
        current_time_ist = datetime.now(IST)

        if not self.tick_buffer:
            logging.debug("No ticks in buffer to process")
            return

        try:
            total_ticks = sum(len(ticks) for ticks in self.tick_buffer.values())
            unique_instruments = len(self.tick_buffer)

            all_ticks = []
            for _, ticks in self.tick_buffer.items():
                all_ticks.extend(ticks)

            try:
                self.processing_queue.put_nowait((all_ticks, current_time_ist))
                logging.info(
                    f"📤 Queued {total_ticks} ticks from {unique_instruments} instruments | "
                    f"Processing queue: {self.processing_queue.qsize()}"
                )
            except queue.Full:
                logging.warning(f"⚠️ Processing queue full! Dropping {total_ticks} ticks")

            self.tick_buffer.clear()

            if self.total_ticks_received % 50000 == 0:
                self.cleanup_old_ohlc_tracking()

        except Exception as e:
            logging.error(f"Error queueing ticks: {e}")

    def log_status_update(self):
        buffer_size = sum(len(v) for v in self.tick_buffer.values())
        active_instruments = len(self.tick_buffer)

        sample_volumes = []
        with self.ohlc_lock:
            for (token, _ts), data in list(self.daily_ohlc.items())[:3]:
                if data["volume"] > 0 and token in self.instruments_data:
                    symbol = self.instruments_data[token]["tradingsymbol"]
                    sample_volumes.append(f"{symbol}:{data['volume']:,}")

        volume_info = f" | Vol: {', '.join(sample_volumes)}" if sample_volumes else " | Vol: checking..."

        logging.info(
            f"📊 STATUS: {self.total_ticks_received:,} received | {self.total_ticks_processed:,} processed | "
            f"{buffer_size} buffered | {active_instruments} active | "
            f"ProcQ: {self.processing_queue.qsize()} | DBQ: {self.db_queue.qsize()} | "
            f"{self.total_records_processed:,} saved | Failed: {self.failed_db_operations}{volume_info}"
        )

    # -----------------------------------------------------
    # CALLBACKS
    # -----------------------------------------------------
    def on_ticks(self, ws, ticks):
        try:
            current_time_ist = datetime.now(IST)
            current_time_utc = current_time_ist.astimezone(UTC)

            self.total_ticks_received += len(ticks)

            if not hasattr(self, "_logged_tick_sample") and ticks:
                logging.info(f"📊 Sample tick data: {ticks[0]}")
                self._logged_tick_sample = True

            if CACHE_ENABLED and cache and cache.is_available():
                for tick in ticks:
                    instrument_token = tick["instrument_token"]
                    if instrument_token in self.instruments_data:
                        instrument_info = self.instruments_data[instrument_token]
                        symbol = instrument_info["tradingsymbol"]

                        tick_data = {
                            "last_price": tick.get("last_price"),
                            "volume": tick.get("volume_traded", 0),
                            "oi": tick.get("oi", 0),
                            "change": tick.get("change", 0),
                            "timestamp": current_time_ist.isoformat(),
                            "instrument_token": instrument_token,
                            "exchange": instrument_info["exchange"]
                        }

                        try:
                            cache.set_tick(symbol, tick_data, ttl=300)
                        except Exception as cache_error:
                            logging.warning(f"⚠️ Cache tick write failed for {symbol}: {cache_error}")

            for tick in ticks:
                instrument_token = tick["instrument_token"]
                if instrument_token in self.instruments_data:
                    self.tick_buffer[instrument_token].append(tick)

            if self.last_status_update is None:
                self.last_status_update = current_time_utc
            if self.last_hour_update is None:
                self.last_hour_update = current_time_utc.replace(minute=0, second=0, microsecond=0)
            if self.last_day_update is None:
                self.last_day_update = current_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            if self.last_interval_update is None:
                self.last_interval_update = current_time_utc

            if (current_time_utc - self.last_status_update).total_seconds() >= 30:
                self.log_status_update()
                self.last_status_update = current_time_utc

            current_hour_utc = current_time_utc.replace(minute=0, second=0, microsecond=0)
            if current_hour_utc > self.last_hour_update:
                logging.info(f"🕐 New hour detected (UTC): {current_hour_utc}")
                self.process_and_save_interval_data()
                self.last_hour_update = current_hour_utc

            current_day_utc = current_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            if current_day_utc > self.last_day_update:
                logging.info(f"📅 New day detected (UTC): {current_day_utc}")
                self.process_and_save_interval_data()
                self.last_day_update = current_day_utc

            if (current_time_utc - self.last_interval_update).total_seconds() >= self.update_interval:
                logging.info(f"⏰ Interval update ({self.update_interval}s): {current_time_utc.strftime('%H:%M:%S')} UTC")
                self.process_and_save_interval_data()
                self.last_interval_update = current_time_utc

        except Exception as e:
            logging.error(f"Error processing ticks: {e}")

    def on_connect(self, ws, response):
        try:
            instrument_tokens = list(self.instruments_data.keys())
            ws.subscribe(instrument_tokens)
            ws.set_mode(ws.MODE_FULL, instrument_tokens)

            mode_label = "mock" if self.use_mock else "real"
            logging.info(f"🚀 Connected and subscribed to {len(instrument_tokens)} instruments ({mode_label} mode)")
            logging.info("📡 Streaming started - waiting for market data...")

            current_time_utc = datetime.now(UTC)
            self.last_status_update = current_time_utc
            self.last_hour_update = current_time_utc.replace(minute=0, second=0, microsecond=0)
            self.last_day_update = current_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            self.last_interval_update = current_time_utc

        except Exception as e:
            logging.error(f"Error on connect: {e}")

    def on_close(self, ws, code, reason):
        logging.warning(f"⚠️ Connection closed: {code} - {reason}")

        if self.use_mock:
            return

        if code in [1006, 1000]:
            logging.info("🔄 Attempting to reconnect in 5 seconds...")
            time_module.sleep(5)
            try:
                logging.info("🔄 Reconnecting...")
                self.start_streaming()
            except Exception as e:
                logging.error(f"Failed to reconnect: {e}")
        else:
            try:
                ws.stop()
            except Exception:
                pass

    def on_error(self, ws, code, reason):
        logging.error(f"❌ Connection error: {code} - {reason}")

    # -----------------------------------------------------
    # START / STOP
    # -----------------------------------------------------
    def start_streaming(self):
        try:
            if self.use_mock:
                self.kws = MockKiteTicker(
                    api_key="mock_api_key",
                    access_token="mock_access_token",
                    instrument_lookup=self.instruments_data,
                    tick_interval=self.mock_tick_interval
                )
            else:
                if not self.access_token:
                    logging.error("No access token available. Please set up Kite connection first.")
                    return
                self.kws = KiteTicker(api_key, self.access_token)

            self.kws.on_ticks = self.on_ticks
            self.kws.on_connect = self.on_connect
            self.kws.on_close = self.on_close
            self.kws.on_error = self.on_error

            mode_label = "mock" if self.use_mock else "real"
            logging.info(f"Starting {mode_label} real-time streaming...")
            self.kws.connect(threaded=False)

        except Exception as e:
            logging.error(f"Error starting streaming: {e}")
            raise

    def cleanup(self):
        if self.cleanup_done and self.use_mock:
            return

        try:
            logging.info("🛑 Starting cleanup...")

            if self.kws:
                try:
                    self.kws.stop()
                except Exception:
                    pass

            if self.tick_buffer:
                logging.info("Processing remaining ticks...")
                self.process_and_save_interval_data()

            if not self.processing_queue.empty():
                logging.info(f"Waiting for {self.processing_queue.qsize()} processing operations...")
            self.processing_queue.join()

            if self.processing_worker_running:
                logging.info("Stopping processing worker...")
                self.processing_worker_running = False
                self.processing_queue.put(None)

                if self.processing_worker_thread:
                    self.processing_worker_thread.join(timeout=10)

            if not self.db_queue.empty():
                logging.info(f"Waiting for {self.db_queue.qsize()} DB operations...")
            self.db_queue.join()

            if self.db_worker_running:
                logging.info(f"Stopping {self.num_db_workers} database workers...")
                self.db_worker_running = False

                for _ in range(self.num_db_workers):
                    self.db_queue.put(None)

                for thread in self.db_worker_threads:
                    thread.join(timeout=10)

            if self.session:
                self.session.close()

            if self.use_mock:
                try:
                    self.restore_mock_run_data()
                    self._cleanup_mock_cache_best_effort()
                except Exception as restore_error:
                    logging.error(f"❌ Restore step failed: {restore_error}")

            logging.info("✅ Cleanup completed")
            logging.info(
                f"📈 Final stats: {self.total_ticks_received:,} received | "
                f"{self.total_ticks_processed:,} processed | "
                f"{self.total_records_processed:,} saved | "
                f"{self.failed_db_operations} failures"
            )
        except Exception as e:
            logging.error(f"Error during cleanup: {e}")

    def _cleanup_mock_cache_best_effort(self):
        """
        Best-effort cache cleanup only if your cache_service supports it.
        Safe no-op otherwise.
        """
        if not (CACHE_ENABLED and cache and hasattr(cache, "is_available") and cache.is_available()):
            return

        try:
            if hasattr(cache, "delete_mock_run"):
                cache.delete_mock_run(self.mock_run_id)
                logging.info("✅ Cleared mock cache via delete_mock_run")
                return

            if hasattr(cache, "flush_mock_keys_for_run"):
                cache.flush_mock_keys_for_run(self.mock_run_id)
                logging.info("✅ Cleared mock cache via flush_mock_keys_for_run")
                return

            logging.info("ℹ️ Cache cleanup method not available; relying on TTL expiry")
        except Exception as e:
            logging.warning(f"⚠️ Best-effort cache cleanup failed: {e}")


# =========================================================
# MANUAL RESTORE
# =========================================================
def restore_mock_run_by_id(run_id):
    conn = None
    cursor = None
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()

        logging.info(f"♻️ Manually restoring mock run: {run_id}")

        cursor.execute("""
            DELETE FROM ohlcv o
            USING ohlcv_mock_written_keys w
            WHERE w.run_id = %s
              AND o.instrument_token = w.instrument_token
              AND o.interval = w.interval
              AND o.ts = w.ts
        """, (run_id,))

        cursor.execute("""
            INSERT INTO ohlcv (
                instrument_token, tradingsymbol, exchange, interval, ts,
                open, high, low, close, volume, oi
            )
            SELECT
                instrument_token, tradingsymbol, exchange, interval, ts,
                open, high, low, close, volume, oi
            FROM ohlcv_mock_backup
            WHERE run_id = %s
              AND existed_before = TRUE
            ON CONFLICT (instrument_token, interval, ts)
            DO UPDATE SET
                tradingsymbol = EXCLUDED.tradingsymbol,
                exchange = EXCLUDED.exchange,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                oi = EXCLUDED.oi
        """, (run_id,))

        cursor.execute("DELETE FROM ohlcv_mock_written_keys WHERE run_id = %s", (run_id,))
        cursor.execute("DELETE FROM ohlcv_mock_backup WHERE run_id = %s", (run_id,))

        conn.commit()
        logging.info("✅ Manual restore complete")
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"❌ Manual restore failed: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)


# =========================================================
# SIGNAL HANDLING
# =========================================================
_global_streamer = None


def _handle_shutdown_signal(signum, frame):
    global _global_streamer
    logging.warning(f"⚠️ Received shutdown signal: {signum}")
    if _global_streamer:
        try:
            _global_streamer.cleanup()
        finally:
            sys.exit(0)


def _atexit_cleanup():
    global _global_streamer
    if _global_streamer:
        try:
            _global_streamer.cleanup()
        except Exception as e:
            logging.error(f"atexit cleanup failed: {e}")


# =========================================================
# MAIN
# =========================================================
def main():
    global _global_streamer
    streamer = None

    try:
        signal.signal(signal.SIGINT, _handle_shutdown_signal)
        signal.signal(signal.SIGTERM, _handle_shutdown_signal)
        atexit.register(_atexit_cleanup)

        # Manual restore modes
        if len(sys.argv) > 1 and sys.argv[1] == "--restore-run":
            if len(sys.argv) < 3:
                logging.error("Usage: python streamer.py --restore-run <run_id>")
                return
            restore_mock_run_by_id(sys.argv[2])
            return

        if len(sys.argv) > 1 and sys.argv[1] == "--restore-last":
            run_id_file = "/tmp/mock_streamer_last_run_id.txt"
            if not os.path.exists(run_id_file):
                logging.error("No last run id file found")
                return
            with open(run_id_file, "r") as f:
                run_id = f.read().strip()
            restore_mock_run_by_id(run_id)
            return

        # Runtime args
        # python streamer.py
        # python streamer.py 15
        # python streamer.py 15 1
        # python streamer.py --mock
        # python streamer.py --mock 15 1
        use_mock = USE_MOCK_STREAM
        update_interval = None
        mock_tick_interval = DEFAULT_MOCK_TICK_INTERVAL

        args = sys.argv[1:]

        if args and args[0] == "--mock":
            use_mock = True
            args = args[1:]
        elif args and args[0] == "--real":
            use_mock = False
            args = args[1:]

        if len(args) > 0:
            try:
                update_interval = int(args[0])
                if update_interval < MIN_UPDATE_INTERVAL:
                    logging.error(f"Update interval must be at least {MIN_UPDATE_INTERVAL} seconds")
                    return
            except ValueError:
                logging.error("First positional arg must be update interval in seconds")
                return

        if len(args) > 1:
            try:
                mock_tick_interval = float(args[1])
            except ValueError:
                logging.error("Second positional arg must be mock tick interval in seconds")
                return

        ist_now = datetime.now(IST)
        utc_now = ist_now.astimezone(UTC)
        market_start = time(9, 15)
        market_end = time(15, 30)

        if not use_mock:
            if not (market_start <= ist_now.time() <= market_end):
                logging.info(
                    f"Outside market hours (IST: {ist_now.strftime('%H:%M:%S')}). "
                    f"Real streamer will run but may not receive data."
                )
            else:
                logging.info(f"Market hours active (IST: {ist_now.strftime('%H:%M:%S')}). Starting real streamer...")
        else:
            logging.info(f"Mock mode active (IST: {ist_now.strftime('%H:%M:%S')}). Starting mock streamer...")

        logging.info(f"Timezone info - IST: {ist_now.strftime('%Y-%m-%d %H:%M:%S %Z')}, UTC: {utc_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logging.info("All database timestamps will be stored in UTC")

        if use_mock:
            logging.info("🧪 Running in MOCK mode against the REAL DB/CACHE")
            logging.info("🧹 DB restore-on-stop is ENABLED")

        streamer = RealTimeStreamer(
            update_interval=update_interval,
            use_mock=use_mock,
            mock_tick_interval=mock_tick_interval
        )

        _global_streamer = streamer
        streamer.start_streaming()

    except KeyboardInterrupt:
        logging.info("Received interrupt signal. Shutting down...")
    except Exception as e:
        logging.error(f"Error in main: {e}")
    finally:
        if streamer:
            streamer.cleanup()


if __name__ == "__main__":
    main()
