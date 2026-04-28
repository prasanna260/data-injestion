"""
Redis Cache Service for Real-Time Market Data
Provides low-latency access to streaming market data
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import redis
from redis.connection import ConnectionPool
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

# Cache TTL settings (in seconds)
TICK_TTL = 300  # 5 minutes for individual ticks
OHLC_TTL = 3600  # 1 hour for OHLC data
INSTRUMENT_LIST_TTL = 86400  # 24 hours for instrument list

# Redis key prefixes
PREFIX_TICK = "tick:"  # tick:SYMBOL or tick:TOKEN
PREFIX_OHLC = "ohlc:"  # ohlc:SYMBOL:INTERVAL
PREFIX_INSTRUMENT = "instrument:"  # instrument:TOKEN
PREFIX_SYMBOL_MAP = "symbol_map:"  # symbol_map:SYMBOL -> TOKEN
PREFIX_MARKET_STATUS = "market:status"
PREFIX_LATEST_PRICES = "latest:prices"  # Hash of all latest prices

class CacheService:
    """Redis-based cache service for market data"""
    
    def __init__(self):
        """Initialize Redis connection pool"""
        self._available = False
        self._last_check = None
        self._check_interval = 60  # Check availability every 60 seconds
        
        try:
            self.pool = ConnectionPool(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True,
                max_connections=50,
                socket_connect_timeout=2,  # Reduced from 5 to 2 seconds
                socket_timeout=2,  # Reduced from 5 to 2 seconds
                retry_on_timeout=False  # Don't retry on timeout
            )
            self.redis_client = redis.Redis(connection_pool=self.pool)
            
            # Test connection
            self.redis_client.ping()
            self._available = True
            self._last_check = datetime.now()
            logger.info(f"✅ Redis connected: {REDIS_HOST}:{REDIS_PORT}")
            
        except redis.ConnectionError as e:
            logger.warning(f"⚠️ Redis connection failed: {e} - Cache will be disabled")
            self.redis_client = None
            self._available = False
        except Exception as e:
            logger.warning(f"⚠️ Redis initialization error: {e} - Cache will be disabled")
            self.redis_client = None
            self._available = False
    
    def is_available(self) -> bool:
        """Check if Redis is available (with caching to avoid repeated pings)"""
        if not self.redis_client:
            return False
        
        # Use cached availability status if checked recently
        now = datetime.now()
        if self._last_check and (now - self._last_check).total_seconds() < self._check_interval:
            return self._available
        
        # Check availability
        try:
            self.redis_client.ping()
            self._available = True
            self._last_check = now
            return True
        except:
            self._available = False
            self._last_check = now
            return False
    
    # ==================== TICK DATA ====================
    
    def set_tick(self, symbol: str, tick_data: Dict[str, Any], ttl: int = TICK_TTL):
        """
        Store latest tick data for a symbol
        
        Args:
            symbol: Trading symbol (e.g., "RELIANCE", "NIFTY 50")
            tick_data: Dict with keys: last_price, volume, oi, timestamp, etc.
            ttl: Time to live in seconds
        """
        if not self.is_available():
            return False
        
        try:
            key = f"{PREFIX_TICK}{symbol}"
            
            # Add timestamp if not present
            if 'timestamp' not in tick_data:
                tick_data['timestamp'] = datetime.now().isoformat()
            
            # Store as JSON
            self.redis_client.setex(
                key,
                ttl,
                json.dumps(tick_data)
            )
            
            # Also update the latest prices hash for quick access
            if 'last_price' in tick_data:
                self.redis_client.hset(
                    PREFIX_LATEST_PRICES,
                    symbol,
                    tick_data['last_price']
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting tick for {symbol}: {e}")
            return False
    
    def get_tick(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get latest tick data for a symbol
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Dict with tick data or None if not found
        """
        if not self.is_available():
            return None
        
        try:
            key = f"{PREFIX_TICK}{symbol}"
            data = self.redis_client.get(key)
            
            if data:
                return json.loads(data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting tick for {symbol}: {e}")
            return None
    
    def get_latest_prices(self, symbols: List[str] = None) -> Dict[str, float]:
        """
        Get latest prices for multiple symbols (very fast)
        
        Args:
            symbols: List of symbols, or None for all
            
        Returns:
            Dict mapping symbol -> price
        """
        if not self.is_available():
            return {}
        
        try:
            if symbols:
                # Get specific symbols
                prices = self.redis_client.hmget(PREFIX_LATEST_PRICES, symbols)
                return {
                    symbol: float(price) if price else None
                    for symbol, price in zip(symbols, prices)
                }
            else:
                # Get all prices
                all_prices = self.redis_client.hgetall(PREFIX_LATEST_PRICES)
                return {
                    symbol: float(price)
                    for symbol, price in all_prices.items()
                }
                
        except Exception as e:
            logger.error(f"Error getting latest prices: {e}")
            return {}
    
    # ==================== OHLC DATA ====================
    
    def set_ohlc(self, symbol: str, interval: str, ohlc_data: Dict[str, Any], ttl: int = OHLC_TTL):
        """
        Store OHLC data for a symbol and interval
        
        Args:
            symbol: Trading symbol
            interval: Time interval (e.g., "1minute", "15minute", "60minute", "1day")
            ohlc_data: Dict with keys: open, high, low, close, volume, oi, timestamp
            ttl: Time to live in seconds
        """
        if not self.is_available():
            return False
        
        try:
            key = f"{PREFIX_OHLC}{symbol}:{interval}"
            
            # Add timestamp if not present
            if 'timestamp' not in ohlc_data:
                ohlc_data['timestamp'] = datetime.now().isoformat()
            
            self.redis_client.setex(
                key,
                ttl,
                json.dumps(ohlc_data)
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting OHLC for {symbol} {interval}: {e}")
            return False
    
    def get_ohlc(self, symbol: str, interval: str) -> Optional[Dict[str, Any]]:
        """
        Get OHLC data for a symbol and interval
        
        Args:
            symbol: Trading symbol
            interval: Time interval
            
        Returns:
            Dict with OHLC data or None if not found
        """
        if not self.is_available():
            return None
        
        try:
            key = f"{PREFIX_OHLC}{symbol}:{interval}"
            data = self.redis_client.get(key)
            
            if data:
                return json.loads(data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting OHLC for {symbol} {interval}: {e}")
            return None
    
    def set_ohlc_batch(self, ohlc_records: List[Dict[str, Any]], ttl: int = OHLC_TTL):
        """
        Store multiple OHLC records in batch (more efficient)
        
        Args:
            ohlc_records: List of dicts with keys: symbol, interval, open, high, low, close, volume, oi, timestamp
            ttl: Time to live in seconds
        """
        if not self.is_available() or not ohlc_records:
            return False
        
        try:
            pipe = self.redis_client.pipeline()
            
            for record in ohlc_records:
                symbol = record.get('tradingsymbol') or record.get('symbol')
                interval = record.get('interval')
                
                if not symbol or not interval:
                    continue
                
                key = f"{PREFIX_OHLC}{symbol}:{interval}"
                
                # Add timestamp if not present
                if 'timestamp' not in record:
                    record['timestamp'] = datetime.now().isoformat()
                
                pipe.setex(key, ttl, json.dumps(record))
            
            pipe.execute()
            return True
            
        except Exception as e:
            logger.error(f"Error setting OHLC batch: {e}")
            return False
    
    # ==================== INSTRUMENT MAPPING ====================
    
    def set_instrument_batch(self, instruments: List[tuple]):
        """
        Store multiple instrument mappings in batch (much faster)
        
        Args:
            instruments: List of tuples (token, symbol, exchange, segment)
        """
        if not self.is_available() or not instruments:
            return False
        
        try:
            pipe = self.redis_client.pipeline()
            
            for token, symbol, exchange, segment in instruments:
                # Store token -> instrument data
                instrument_key = f"{PREFIX_INSTRUMENT}{token}"
                instrument_data = {
                    'token': token,
                    'symbol': symbol,
                    'exchange': exchange,
                    'segment': segment
                }
                pipe.setex(
                    instrument_key,
                    INSTRUMENT_LIST_TTL,
                    json.dumps(instrument_data)
                )
                
                # Store symbol -> token mapping
                symbol_key = f"{PREFIX_SYMBOL_MAP}{symbol}"
                pipe.setex(symbol_key, INSTRUMENT_LIST_TTL, str(token))
            
            pipe.execute()
            return True
            
        except Exception as e:
            logger.error(f"Error setting instrument batch: {e}")
            return False
    
    def set_instrument(self, token: int, symbol: str, exchange: str, segment: str):
        """
        Store instrument mapping (single item - use set_instrument_batch for multiple)
        
        Args:
            token: Instrument token
            symbol: Trading symbol
            exchange: Exchange (NSE, BSE, etc.)
            segment: Segment (INDICES, EQ, etc.)
        """
        if not self.is_available():
            return False
        
        try:
            # Store token -> instrument data
            instrument_key = f"{PREFIX_INSTRUMENT}{token}"
            instrument_data = {
                'token': token,
                'symbol': symbol,
                'exchange': exchange,
                'segment': segment
            }
            self.redis_client.setex(
                instrument_key,
                INSTRUMENT_LIST_TTL,
                json.dumps(instrument_data)
            )
            
            # Store symbol -> token mapping
            symbol_key = f"{PREFIX_SYMBOL_MAP}{symbol}"
            self.redis_client.setex(symbol_key, INSTRUMENT_LIST_TTL, str(token))
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting instrument {symbol}: {e}")
            return False
    
    def get_token_by_symbol(self, symbol: str) -> Optional[int]:
        """Get instrument token by symbol"""
        if not self.is_available():
            return None
        
        try:
            key = f"{PREFIX_SYMBOL_MAP}{symbol}"
            token = self.redis_client.get(key)
            return int(token) if token else None
            
        except Exception as e:
            logger.error(f"Error getting token for {symbol}: {e}")
            return None
    
    def get_instrument_by_token(self, token: int) -> Optional[Dict[str, Any]]:
        """Get instrument data by token"""
        if not self.is_available():
            return None
        
        try:
            key = f"{PREFIX_INSTRUMENT}{token}"
            data = self.redis_client.get(key)
            return json.loads(data) if data else None
            
        except Exception as e:
            logger.error(f"Error getting instrument for token {token}: {e}")
            return None
    
    # ==================== MARKET STATUS ====================
    
    def set_market_status(self, is_open: bool, message: str = ""):
        """Set market status"""
        if not self.is_available():
            return False
        
        try:
            status_data = {
                'is_open': is_open,
                'message': message,
                'timestamp': datetime.now().isoformat()
            }
            self.redis_client.setex(
                PREFIX_MARKET_STATUS,
                300,  # 5 minutes TTL
                json.dumps(status_data)
            )
            return True
            
        except Exception as e:
            logger.error(f"Error setting market status: {e}")
            return False
    
    def get_market_status(self) -> Optional[Dict[str, Any]]:
        """Get market status"""
        if not self.is_available():
            return None
        
        try:
            data = self.redis_client.get(PREFIX_MARKET_STATUS)
            return json.loads(data) if data else None
            
        except Exception as e:
            logger.error(f"Error getting market status: {e}")
            return None
    
    # ==================== UTILITY ====================
    
    def clear_all(self):
        """Clear all cache (use with caution!)"""
        if not self.is_available():
            return False
        
        try:
            self.redis_client.flushdb()
            logger.info("🗑️ Cache cleared")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        if not self.is_available():
            return {'available': False}
        
        try:
            info = self.redis_client.info()
            return {
                'available': True,
                'connected_clients': info.get('connected_clients', 0),
                'used_memory_human': info.get('used_memory_human', 'N/A'),
                'total_keys': self.redis_client.dbsize(),
                'uptime_seconds': info.get('uptime_in_seconds', 0)
            }
            
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {'available': False, 'error': str(e)}


# Global cache instance
cache = CacheService()


# Convenience functions
def get_cached_tick(symbol: str) -> Optional[Dict[str, Any]]:
    """Get cached tick data for a symbol"""
    return cache.get_tick(symbol)


def get_cached_ohlc(symbol: str, interval: str) -> Optional[Dict[str, Any]]:
    """Get cached OHLC data for a symbol and interval"""
    return cache.get_ohlc(symbol, interval)


def get_cached_latest_prices(symbols: List[str] = None) -> Dict[str, float]:
    """Get latest prices for symbols"""
    return cache.get_latest_prices(symbols)


def is_cache_available() -> bool:
    """Check if cache is available"""
    return cache.is_available()
