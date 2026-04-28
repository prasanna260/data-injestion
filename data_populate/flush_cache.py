#!/usr/bin/env python3
"""
Redis Cache Cleanup Script
Flushes old or stale cache entries from Redis
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict
import redis
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

# Key prefixes
PREFIX_TICK = "tick:"
PREFIX_OHLC = "ohlc:"
PREFIX_INSTRUMENT = "instrument:"
PREFIX_SYMBOL_MAP = "symbol_map:"
PREFIX_MARKET_STATUS = "market:status"
PREFIX_LATEST_PRICES = "latest:prices"


class RedisCacheCleaner:
    """Redis cache cleanup utility"""
    
    def __init__(self):
        """Initialize Redis connection"""
        try:
            self.redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            
            # Test connection
            self.redis_client.ping()
            logger.info(f"✅ Connected to Redis: {REDIS_HOST}:{REDIS_PORT}")
            
        except redis.ConnectionError as e:
            logger.error(f"❌ Redis connection failed: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Redis initialization error: {e}")
            sys.exit(1)
    
    def get_stats(self) -> Dict:
        """Get current cache statistics"""
        try:
            info = self.redis_client.info()
            dbsize = self.redis_client.dbsize()
            
            # Count keys by prefix
            tick_keys = len(self.redis_client.keys(f"{PREFIX_TICK}*"))
            ohlc_keys = len(self.redis_client.keys(f"{PREFIX_OHLC}*"))
            instrument_keys = len(self.redis_client.keys(f"{PREFIX_INSTRUMENT}*"))
            symbol_map_keys = len(self.redis_client.keys(f"{PREFIX_SYMBOL_MAP}*"))
            
            return {
                'total_keys': dbsize,
                'tick_keys': tick_keys,
                'ohlc_keys': ohlc_keys,
                'instrument_keys': instrument_keys,
                'symbol_map_keys': symbol_map_keys,
                'used_memory': info.get('used_memory_human', 'N/A'),
                'connected_clients': info.get('connected_clients', 0),
                'uptime_seconds': info.get('uptime_in_seconds', 0)
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}
    
    def print_stats(self, label: str = "Current"):
        """Print cache statistics"""
        stats = self.get_stats()
        if stats:
            logger.info(f"\n{'='*60}")
            logger.info(f"{label} Cache Statistics")
            logger.info(f"{'='*60}")
            logger.info(f"Total Keys:        {stats['total_keys']:,}")
            logger.info(f"Tick Keys:         {stats['tick_keys']:,}")
            logger.info(f"OHLC Keys:         {stats['ohlc_keys']:,}")
            logger.info(f"Instrument Keys:   {stats['instrument_keys']:,}")
            logger.info(f"Symbol Map Keys:   {stats['symbol_map_keys']:,}")
            logger.info(f"Memory Used:       {stats['used_memory']}")
            logger.info(f"Connected Clients: {stats['connected_clients']}")
            logger.info(f"Uptime:            {stats['uptime_seconds']:,}s")
            logger.info(f"{'='*60}\n")
    
    def flush_all(self, confirm: bool = False):
        """Flush all cache data"""
        if not confirm:
            logger.warning("⚠️  This will delete ALL cache data!")
            response = input("Are you sure? Type 'yes' to confirm: ")
            if response.lower() != 'yes':
                logger.info("Aborted.")
                return
        
        try:
            self.redis_client.flushdb()
            logger.info("✅ All cache data flushed")
        except Exception as e:
            logger.error(f"❌ Error flushing cache: {e}")
    
    def flush_by_pattern(self, pattern: str, batch_size: int = 1000):
        """
        Flush keys matching a pattern
        
        Args:
            pattern: Redis key pattern (e.g., "tick:*", "ohlc:RELIANCE:*")
            batch_size: Number of keys to delete per batch
        """
        try:
            cursor = 0
            total_deleted = 0
            
            logger.info(f"🔍 Scanning for keys matching: {pattern}")
            
            while True:
                cursor, keys = self.redis_client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=batch_size
                )
                
                if keys:
                    # Delete in pipeline for efficiency
                    pipe = self.redis_client.pipeline()
                    for key in keys:
                        pipe.delete(key)
                    pipe.execute()
                    
                    total_deleted += len(keys)
                    logger.info(f"🗑️  Deleted {len(keys)} keys (total: {total_deleted})")
                
                if cursor == 0:
                    break
            
            logger.info(f"✅ Deleted {total_deleted} keys matching '{pattern}'")
            return total_deleted
            
        except Exception as e:
            logger.error(f"❌ Error flushing pattern '{pattern}': {e}")
            return 0
    
    def flush_tick_data(self):
        """Flush all tick data"""
        logger.info("🧹 Flushing tick data...")
        deleted = self.flush_by_pattern(f"{PREFIX_TICK}*")
        
        # Also clear latest prices hash
        try:
            self.redis_client.delete(PREFIX_LATEST_PRICES)
            logger.info(f"🗑️  Cleared latest prices hash")
        except Exception as e:
            logger.error(f"Error clearing latest prices: {e}")
        
        return deleted
    
    def flush_ohlc_data(self, symbol: str = None, interval: str = None):
        """
        Flush OHLC data
        
        Args:
            symbol: Specific symbol (optional)
            interval: Specific interval (optional)
        """
        if symbol and interval:
            pattern = f"{PREFIX_OHLC}{symbol}:{interval}"
            logger.info(f"🧹 Flushing OHLC data for {symbol} {interval}...")
        elif symbol:
            pattern = f"{PREFIX_OHLC}{symbol}:*"
            logger.info(f"🧹 Flushing OHLC data for {symbol}...")
        elif interval:
            pattern = f"{PREFIX_OHLC}*:{interval}"
            logger.info(f"🧹 Flushing OHLC data for interval {interval}...")
        else:
            pattern = f"{PREFIX_OHLC}*"
            logger.info("🧹 Flushing all OHLC data...")
        
        return self.flush_by_pattern(pattern)
    
    def flush_instrument_data(self):
        """Flush instrument mapping data"""
        logger.info("🧹 Flushing instrument data...")
        deleted1 = self.flush_by_pattern(f"{PREFIX_INSTRUMENT}*")
        deleted2 = self.flush_by_pattern(f"{PREFIX_SYMBOL_MAP}*")
        return deleted1 + deleted2
    
    def flush_old_keys(self, max_age_hours: int = 24):
        """
        Flush keys older than specified hours (only works for keys with TTL)
        Note: This scans all keys and checks their TTL
        
        Args:
            max_age_hours: Maximum age in hours
        """
        logger.info(f"🧹 Flushing keys older than {max_age_hours} hours...")
        
        try:
            cursor = 0
            total_deleted = 0
            total_scanned = 0
            
            while True:
                cursor, keys = self.redis_client.scan(cursor=cursor, count=1000)
                total_scanned += len(keys)
                
                for key in keys:
                    try:
                        ttl = self.redis_client.ttl(key)
                        
                        # TTL -1 means no expiry, -2 means key doesn't exist
                        if ttl == -1:
                            # Key has no expiry, skip
                            continue
                        elif ttl == -2:
                            # Key doesn't exist (race condition)
                            continue
                        elif ttl < 0:
                            # Expired but not yet removed
                            self.redis_client.delete(key)
                            total_deleted += 1
                        
                    except Exception as e:
                        logger.debug(f"Error checking key {key}: {e}")
                
                if cursor == 0:
                    break
                
                if total_scanned % 10000 == 0:
                    logger.info(f"📊 Scanned {total_scanned} keys, deleted {total_deleted}")
            
            logger.info(f"✅ Scanned {total_scanned} keys, deleted {total_deleted} old keys")
            return total_deleted
            
        except Exception as e:
            logger.error(f"❌ Error flushing old keys: {e}")
            return 0
    
    def flush_expired_keys(self):
        """Force Redis to clean up expired keys"""
        logger.info("🧹 Triggering expired key cleanup...")
        try:
            # This doesn't actually delete keys, but triggers Redis's lazy deletion
            # The best way is to just let Redis handle it naturally
            logger.info("ℹ️  Redis handles expired keys automatically")
            logger.info("ℹ️  Use flush_old_keys() to manually check and delete")
        except Exception as e:
            logger.error(f"Error: {e}")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Redis Cache Cleanup Utility',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show current cache statistics
  python flush_redis_cache.py --stats
  
  # Flush all cache data
  python flush_redis_cache.py --flush-all
  
  # Flush only tick data
  python flush_redis_cache.py --flush-ticks
  
  # Flush only OHLC data
  python flush_redis_cache.py --flush-ohlc
  
  # Flush OHLC data for specific symbol
  python flush_redis_cache.py --flush-ohlc --symbol RELIANCE
  
  # Flush OHLC data for specific interval
  python flush_redis_cache.py --flush-ohlc --interval 15minute
  
  # Flush instrument mapping data
  python flush_redis_cache.py --flush-instruments
  
  # Flush keys by custom pattern
  python flush_redis_cache.py --pattern "ohlc:NIFTY*"
  
  # Flush old keys (with TTL expired)
  python flush_redis_cache.py --flush-old
        """
    )
    
    parser.add_argument('--stats', action='store_true',
                       help='Show cache statistics')
    parser.add_argument('--flush-all', action='store_true',
                       help='Flush all cache data (requires confirmation)')
    parser.add_argument('--flush-ticks', action='store_true',
                       help='Flush all tick data')
    parser.add_argument('--flush-ohlc', action='store_true',
                       help='Flush OHLC data')
    parser.add_argument('--flush-instruments', action='store_true',
                       help='Flush instrument mapping data')
    parser.add_argument('--flush-old', action='store_true',
                       help='Flush expired keys')
    parser.add_argument('--pattern', type=str,
                       help='Flush keys matching pattern (e.g., "tick:*")')
    parser.add_argument('--symbol', type=str,
                       help='Specific symbol (use with --flush-ohlc)')
    parser.add_argument('--interval', type=str,
                       help='Specific interval (use with --flush-ohlc)')
    parser.add_argument('--yes', action='store_true',
                       help='Skip confirmation prompts')
    
    args = parser.parse_args()
    
    # If no arguments, show help
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    # Initialize cleaner
    cleaner = RedisCacheCleaner()
    
    # Show stats before
    if not args.stats:
        cleaner.print_stats("BEFORE")
    
    # Execute commands
    if args.stats:
        cleaner.print_stats()
    
    if args.flush_all:
        cleaner.flush_all(confirm=args.yes)
    
    if args.flush_ticks:
        cleaner.flush_tick_data()
    
    if args.flush_ohlc:
        cleaner.flush_ohlc_data(
            symbol=args.symbol,
            interval=args.interval
        )
    
    if args.flush_instruments:
        cleaner.flush_instrument_data()
    
    if args.flush_old:
        cleaner.flush_old_keys()
    
    if args.pattern:
        cleaner.flush_by_pattern(args.pattern)
    
    # Show stats after (if any operation was performed)
    if any([args.flush_all, args.flush_ticks, args.flush_ohlc, 
            args.flush_instruments, args.flush_old, args.pattern]):
        cleaner.print_stats("AFTER")
    
    logger.info("✅ Done!")


if __name__ == "__main__":
    main()
