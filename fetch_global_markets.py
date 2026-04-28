#!/usr/bin/env python3
"""
Fetch global markets overview from Yahoo Finance and write snapshot JSON.

Output file (default): ./global_markets_snapshot.json
Override with: --output /path/to/global_markets_snapshot.json
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import redis
import yfinance as yf


@dataclass(frozen=True)
class FeedItem:
    name: str
    symbol: str
    precision: int = 2
    suffix: str = ""
    currency: Optional[str] = None
    fallback_symbols: Tuple[str, ...] = ()


@dataclass(frozen=True)
class RegionalIndex:
    item: FeedItem
    region: str
    timezone: str
    open_minutes: int
    close_minutes: int
    priority: int = 0


BASE_CATEGORIES: Dict[str, Tuple[str, List[FeedItem]]] = {
    "bonds": (
        "Bonds",
        [
            FeedItem("US 13W T-Bill", "^IRX", 3, "%", "USD"),
            FeedItem("US 5Y", "^FVX", 3, "%", "USD"),
            FeedItem("US 10Y", "^TNX", 3, "%", "USD"),
            FeedItem("US 30Y", "^TYX", 3, "%", "USD"),
            FeedItem("US Corp IG", "LQD", 2, "", "USD"),
            FeedItem("US High Yield", "HYG", 2, "", "USD"),
            FeedItem("US 20Y+", "TLT", 2, "", "USD"),
            FeedItem("US 7-10Y", "IEF", 2, "", "USD"),
            FeedItem("US 1-3Y", "SHY", 2, "", "USD"),
            FeedItem("US Agg Bond", "AGG", 2, "", "USD"),
            FeedItem("US Total Bond", "BND", 2, "", "USD"),
            FeedItem("Intl Ex-US Bond", "BNDX", 2, "", "USD"),
            FeedItem("Intl Treasury", "BWX", 2, "", "USD"),
            FeedItem("Emerging Bonds", "EMB", 2, "", "USD"),
            FeedItem("Int'l Govt Bond", "IGOV", 2, "", "USD"),
        ],
    ),
    "currencies": (
        "Currencies",
        [
            FeedItem("USD/INR", "USDINR=X", 4, "", "INR"),
            FeedItem("EUR/INR", "EURINR=X", 4, "", "INR"),
            FeedItem("GBP/INR", "GBPINR=X", 4, "", "INR"),
            FeedItem("JPY/INR", "JPYINR=X", 4, "", "INR"),
            FeedItem("EUR/USD", "EURUSD=X", 4, "", "USD"),
            FeedItem("GBP/USD", "GBPUSD=X", 4, "", "USD"),
            FeedItem("USD/JPY", "USDJPY=X", 4, "", "JPY"),
            FeedItem("AUD/USD", "AUDUSD=X", 4, "", "USD"),
            FeedItem("USD/CAD", "USDCAD=X", 4, "", "CAD"),
            FeedItem("USD/CHF", "USDCHF=X", 4, "", "CHF"),
            FeedItem("NZD/USD", "NZDUSD=X", 4, "", "USD"),
            FeedItem("EUR/JPY", "EURJPY=X", 4, "", "JPY"),
            FeedItem("GBP/JPY", "GBPJPY=X", 4, "", "JPY"),
            FeedItem("EUR/GBP", "EURGBP=X", 4, "", "GBP"),
            FeedItem("USD/CNY", "USDCNY=X", 4, "", "CNY"),
            FeedItem("USD/SGD", "USDSGD=X", 4, "", "SGD"),
        ],
    ),
    "commodities": (
        "Commodities",
        [
            FeedItem("Gold", "GC=F", 2, "", "USD"),
            FeedItem("Silver", "SI=F", 2, "", "USD"),
            FeedItem("Crude Oil", "CL=F", 2, "", "USD"),
            FeedItem("Natural Gas", "NG=F", 3, "", "USD"),
            FeedItem("Brent Crude", "BZ=F", 2, "", "USD"),
            FeedItem("Copper", "HG=F", 3, "", "USD"),
            FeedItem("Platinum", "PL=F", 2, "", "USD"),
            FeedItem("Palladium", "PA=F", 2, "", "USD"),
            FeedItem("Corn", "ZC=F", 2, "", "USD"),
            FeedItem("Wheat", "ZW=F", 2, "", "USD"),
            FeedItem("Soybeans", "ZS=F", 2, "", "USD"),
            FeedItem("Coffee", "KC=F", 2, "", "USD"),
            FeedItem("Sugar", "SB=F", 2, "", "USD"),
            FeedItem("Cotton", "CT=F", 2, "", "USD"),
            FeedItem("Cocoa", "CC=F", 2, "", "USD"),
        ],
    ),
}

REGIONAL_INDEX_POOL: List[RegionalIndex] = [
    # Asia
    RegionalIndex(FeedItem("Nikkei 225", "^N225", 2, "", "JPY"), "Asia", "Asia/Tokyo", 9 * 60, 15 * 60, 1),
    RegionalIndex(FeedItem("Hang Seng", "^HSI", 2, "", "HKD"), "Asia", "Asia/Hong_Kong", 9 * 60 + 30, 16 * 60, 2),
    RegionalIndex(FeedItem("Shanghai Composite", "000001.SS", 2, "", "CNY", ("^SSEC",)), "Asia", "Asia/Shanghai", 9 * 60 + 30, 15 * 60, 3),
    RegionalIndex(FeedItem("ASX 200", "^AXJO", 2, "", "AUD"), "Asia", "Australia/Sydney", 10 * 60, 16 * 60, 4),
    RegionalIndex(FeedItem("KOSPI", "^KS11", 2, "", "KRW", ("^KQ11",)), "Asia", "Asia/Seoul", 9 * 60, 15 * 60 + 30, 5),
    RegionalIndex(FeedItem("Straits Times", "^STI", 2, "", "SGD"), "Asia", "Asia/Singapore", 9 * 60, 17 * 60, 6),
    # Europe
    RegionalIndex(FeedItem("FTSE 100", "^FTSE", 2, "", "GBP"), "Europe", "Europe/London", 8 * 60, 16 * 60 + 30, 1),
    RegionalIndex(FeedItem("DAX", "^GDAXI", 2, "", "EUR"), "Europe", "Europe/Berlin", 9 * 60, 17 * 60 + 30, 2),
    RegionalIndex(FeedItem("CAC 40", "^FCHI", 2, "", "EUR"), "Europe", "Europe/Paris", 9 * 60, 17 * 60 + 30, 3),
    RegionalIndex(FeedItem("IBEX 35", "^IBEX", 2, "", "EUR"), "Europe", "Europe/Madrid", 9 * 60, 17 * 60 + 30, 4),
    RegionalIndex(FeedItem("FTSE MIB", "FTSEMIB.MI", 2, "", "EUR"), "Europe", "Europe/Rome", 9 * 60, 17 * 60 + 30, 5),
    # US
    RegionalIndex(FeedItem("S&P 500", "^GSPC", 2, "", "USD", ("SPY",)), "US", "America/New_York", 9 * 60 + 30, 16 * 60, 1),
    RegionalIndex(FeedItem("Nasdaq", "^IXIC", 2, "", "USD", ("QQQ",)), "US", "America/New_York", 9 * 60 + 30, 16 * 60, 2),
    RegionalIndex(FeedItem("Dow Jones", "^DJI", 2, "", "USD", ("DIA",)), "US", "America/New_York", 9 * 60 + 30, 16 * 60, 3),
    RegionalIndex(FeedItem("Russell 2000", "^RUT", 2, "", "USD", ("IWM",)), "US", "America/New_York", 9 * 60 + 30, 16 * 60, 4),
    RegionalIndex(FeedItem("TSX Composite", "^GSPTSE", 2, "", "CAD"), "US", "America/Toronto", 9 * 60 + 30, 16 * 60, 5),
    RegionalIndex(FeedItem("Bovespa", "^BVSP", 2, "", "BRL"), "US", "America/Sao_Paulo", 10 * 60, 17 * 60, 6),
]

GLOBAL_MARKET_CORE_SYMBOLS: Tuple[str, ...] = (
    "^GSPC",  # US
    "^IXIC",
    "^DJI",
    "^RUT",
    "^FTSE",  # Europe
    "^GDAXI",
    "^FCHI",
    "^IBEX",
    "^N225",  # Asia
    "^HSI",
    "^AXJO",
    "^STI",
)


def _format_value(value: Optional[float], precision: int, suffix: str = "") -> str:
    if value is None:
        return "-"
    formatted = f"{value:,.{precision}f}"
    return f"{formatted}{suffix}"


def _extract_latest_prev_from_series(frame) -> Tuple[Optional[float], Optional[float]]:
    if frame is None or frame.empty:
        return None, None

    closes = frame["Close"].dropna()
    if closes.empty:
        return None, None
    elif len(closes) == 1:
        return float(closes.iloc[-1]), None
    else:
        return float(closes.iloc[-1]), float(closes.iloc[-2])


def _row_from_series(frame, item: FeedItem) -> Dict:
    latest, prev = _extract_latest_prev_from_series(frame)

    # Fallback path for symbols that intermittently return empty frames (common for some futures).
    if latest is None:
        symbols_to_try = (item.symbol,) + item.fallback_symbols
        for symbol in symbols_to_try:
            ticker = yf.Ticker(symbol)
            fallback_frame = ticker.history(period="1mo", interval="1d", auto_adjust=False)
            latest, prev = _extract_latest_prev_from_series(fallback_frame)
            if latest is not None:
                break

            try:
                fast_info = getattr(ticker, "fast_info", {}) or {}
                fi_latest = fast_info.get("lastPrice")
                fi_prev = fast_info.get("previousClose")
                if fi_latest is not None:
                    latest = float(fi_latest)
                if fi_prev is not None:
                    prev = float(fi_prev)
            except Exception:
                pass

            if latest is not None:
                break

    change = (latest - prev) if latest is not None and prev is not None else None
    change_percent = ((change / prev) * 100.0) if change is not None and prev else None

    return {
        "name": item.name,
        "symbol": item.symbol,
        "value": latest,
        "display_value": _format_value(latest, item.precision, item.suffix),
        "change": change,
        "change_percent": change_percent,
        "currency": item.currency,
    }


def _minutes_until_next_open(
    now_local: datetime,
    open_minutes: int,
    close_minutes: int,
) -> int:
    weekday = now_local.weekday()
    current_minutes = now_local.hour * 60 + now_local.minute

    # Weekday and before open.
    if weekday < 5 and current_minutes < open_minutes:
        return open_minutes - current_minutes

    # Move to next valid weekday.
    days_ahead = 1
    while True:
        next_weekday = (weekday + days_ahead) % 7
        if next_weekday < 5:
            break
        days_ahead += 1
    return ((24 * 60 - current_minutes) + (days_ahead - 1) * 24 * 60 + open_minutes)


def _is_market_open(
    now_utc: datetime,
    timezone_name: str,
    open_minutes: int,
    close_minutes: int,
) -> Tuple[bool, int]:
    now_local = now_utc.astimezone(ZoneInfo(timezone_name))
    weekday = now_local.weekday()
    current_minutes = now_local.hour * 60 + now_local.minute

    if weekday >= 5:
        return False, _minutes_until_next_open(now_local, open_minutes, close_minutes)

    is_open = open_minutes <= current_minutes <= close_minutes
    if is_open:
        return True, close_minutes - current_minutes
    return False, _minutes_until_next_open(now_local, open_minutes, close_minutes)


def _select_global_market_items(now_utc: datetime, max_items: int = 15) -> List[FeedItem]:
    scored = []
    for index in REGIONAL_INDEX_POOL:
        is_open, distance_metric = _is_market_open(
            now_utc, index.timezone, index.open_minutes, index.close_minutes
        )
        scored.append(
            {
                "index": index,
                "is_open": is_open,
                "distance": distance_metric,
            }
        )

    selected: List[FeedItem] = []
    selected_symbols = set()

    # First pass: one per open region (Asia/Europe/US) to keep session-balanced.
    for region in ("Asia", "Europe", "US"):
        region_open = [
            s for s in scored
            if s["is_open"] and s["index"].region == region
        ]
        region_open.sort(key=lambda s: (s["index"].priority, s["distance"]))
        if region_open:
            item = region_open[0]["index"].item
            selected.append(item)
            selected_symbols.add(item.symbol)

    # Second pass: fill remaining with open markets first, then nearest next-open.
    remaining_candidates = [
        s for s in scored if s["index"].item.symbol not in selected_symbols
    ]
    remaining_candidates.sort(
        key=lambda s: (
            0 if s["is_open"] else 1,
            s["distance"],
            s["index"].priority,
        )
    )
    for candidate in remaining_candidates:
        if len(selected) >= max_items:
            break
        item = candidate["index"].item
        selected.append(item)
        selected_symbols.add(item.symbol)

    # Always include core benchmark indices so tabs never degrade to placeholders.
    by_symbol = {r.item.symbol: r.item for r in REGIONAL_INDEX_POOL}
    prioritized: List[FeedItem] = []
    seen = set()

    for symbol in GLOBAL_MARKET_CORE_SYMBOLS:
        item = by_symbol.get(symbol)
        if item and item.symbol not in seen:
            prioritized.append(item)
            seen.add(item.symbol)

    for item in selected:
        if item.symbol not in seen:
            prioritized.append(item)
            seen.add(item.symbol)

    return prioritized[:max_items]


def build_snapshot() -> Dict:
    now_utc = datetime.now(timezone.utc)
    global_market_items = _select_global_market_items(now_utc)
    catalog: Dict[str, Tuple[str, List[FeedItem]]] = {
        "global_market": ("Global Market", global_market_items),
        **BASE_CATEGORIES,
    }

    ticker_to_item: Dict[str, FeedItem] = {}
    for _, (_, items) in catalog.items():
        for item in items:
            ticker_to_item[item.symbol] = item

    tickers = list(ticker_to_item.keys())
    data = yf.download(
        tickers=tickers,
        period="10d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=True,
    )

    categories: List[Dict] = []
    for key, (title, items) in catalog.items():
        category_items = []
        for item in items:
            frame = None
            try:
                # Multi-index format: data[item.symbol]
                frame = data[item.symbol]
            except Exception:
                frame = yf.Ticker(item.symbol).history(period="10d", interval="1d", auto_adjust=False)

            try:
                category_items.append(_row_from_series(frame, item))
            except Exception as e:
                print(f"Warning: Failed to process {item.symbol}: {e}")
                # Add a placeholder with None values
                category_items.append({
                    "name": item.name,
                    "symbol": item.symbol,
                    "value": None,
                    "display_value": "-",
                    "change": None,
                    "change_percent": None,
                    "currency": item.currency,
                })

        categories.append(
            {
                "key": key,
                "title": title,
                "items": category_items,
            }
        )

    return {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "source": "yfinance",
        "categories": categories,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch global markets snapshot from yfinance")
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output JSON file path",
    )
    parser.add_argument(
        "--redis-key",
        default=os.getenv("REDIS_GLOBAL_MARKETS_KEY", "landing:global_markets:overview"),
        help="Redis key to store snapshot JSON",
    )
    args = parser.parse_args()

    snapshot = build_snapshot()

    redis_host = os.getenv("REDIS_HOST", "3.109.202.190")
    redis_port = int(os.getenv("REDIS_PORT", "28741"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    redis_password = os.getenv("REDIS_PASSWORD", "w7i&oj@_rB7Q2#B#f0zO7L0C_l)DL=$ohGL!oEnI")
    redis_ttl = int(os.getenv("REDIS_GLOBAL_MARKETS_TTL_SECONDS", "600"))

    client = redis.Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    client.ping()
    client.setex(args.redis_key, redis_ttl, json.dumps(snapshot, separators=(",", ":")))
    print(f"Wrote snapshot to Redis key: {args.redis_key} (ttl={redis_ttl}s)")

    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        print(f"Also wrote snapshot file: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
