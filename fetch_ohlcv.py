# fetch_ohlcv.py
import os
import time
import logging
import pandas as pd
import requests
from datetime import datetime, timedelta
from pathlib import Path

# ==============================
# ✅ CONFIG
# ==============================
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "LTCUSDT", "QNTUSDT"]
TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
OUT_DIR = Path("data/ohlcv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

BINANCE_API_URL = "https://data-api.binance.vision/api/v3/klines"
BINANCE_MAX_LIMIT = 1000
RETRY_LIMIT = 4
REQUEST_SLEEP = 0.15
INCREMENTAL_HOURS = 12  # periodic incremental fetch window

# 🧠 Dynamic backfill map (per interval)
HISTORICAL_DEPTH = {
    "1m": timedelta(days=365 * 0.5),   # ~6 months
    "5m": timedelta(days=365),         # ~1 year
    "15m": timedelta(days=365 * 2),    # ~2 years
    "30m": timedelta(days=365 * 2),    # ~2 years
    "1h": timedelta(days=365 * 3),     # ~3 years
    "4h": timedelta(days=365 * 4),     # ~4 years
    "1d": timedelta(days=365 * 5),     # ~5 years
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ==============================
# ✅ FETCH FROM BINANCE
# ==============================
def fetch_klines(symbol, interval, start_ts=None, end_ts=None, limit=BINANCE_MAX_LIMIT):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ts:
        params["startTime"] = int(start_ts.timestamp() * 1000)
    if end_ts:
        params["endTime"] = int(end_ts.timestamp() * 1000)

    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            r = requests.get(BINANCE_API_URL, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "code" in data:
                raise Exception(data.get("msg", "API error"))
            return data
        except Exception as e:
            logging.warning(f"[{symbol}-{interval}] Retry {attempt}/{RETRY_LIMIT} failed: {e}")
            time.sleep(attempt * 1.5)
    logging.error(f"[{symbol}-{interval}] Failed after {RETRY_LIMIT} retries.")
    return []

# ==============================
# ✅ TO DATAFRAME
# ==============================
def to_df(klines, symbol, interval):
    if not klines:
        return pd.DataFrame()
    df = pd.DataFrame(
        klines,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "num_trades",
            "taker_buy_base", "taker_buy_quote", "ignore"
        ]
    )
    df["symbol"] = symbol
    df["interval"] = interval
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
    df["num_trades"] = df["num_trades"].astype(int)
    return df[["symbol", "interval", "open_time", "close_time", "open", "high", "low", "close", "volume", "num_trades"]]

# ==============================
# ✅ MERGE + SAVE
# ==============================
def save_parquet(df, symbol, interval):
    path = OUT_DIR / f"{symbol}_{interval}.parquet"
    if path.exists():
        old = pd.read_parquet(path)
        combined = pd.concat([old, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["symbol", "interval", "open_time"]).sort_values("open_time")
    else:
        combined = df
    combined.to_parquet(path, index=False)
    logging.info(f"💾 {symbol}-{interval}: saved {len(combined)} total rows")

# ==============================
# ✅ DETERMINE FETCH MODE
# ==============================
def determine_fetch_range(symbol, interval):
    path = OUT_DIR / f"{symbol}_{interval}.parquet"
    now = datetime.utcnow()

    if not path.exists():
        # 🔹 Historical backfill (first time)
        backfill_start = now - HISTORICAL_DEPTH.get(interval, timedelta(days=365))
        logging.info(f"🕰️ {symbol}-{interval}: Performing historical backfill ({backfill_start.date()} → {now.date()})")
        return backfill_start, now
    else:
        # 🔹 Incremental mode (subsequent 12-hour updates)
        df = pd.read_parquet(path)
        last_ts = df["open_time"].max()
        start_time = last_ts + timedelta(milliseconds=1)
        if (now - start_time) < timedelta(hours=INCREMENTAL_HOURS):
            logging.info(f"✅ {symbol}-{interval}: Already up to date (last={last_ts})")
            return None, None
        logging.info(f"⏩ {symbol}-{interval}: Incremental fetch from {start_time} to {now}")
        return start_time, now

# ==============================
# ✅ FETCH LOOP
# ==============================
def run_fetch():
    for sym in SYMBOLS:
        for interval in TIMEFRAMES:
            start_time, end_time = determine_fetch_range(sym, interval)
            if not start_time or not end_time:
                continue

            all_dfs = []
            current = start_time
            while current < end_time:
                klines = fetch_klines(sym, interval, current, end_time)
                if not klines:
                    break
                df = to_df(klines, sym, interval)
                if df.empty:
                    break
                all_dfs.append(df)
                last_open = df["open_time"].iloc[-1]
                current = last_open + timedelta(milliseconds=1)
                time.sleep(REQUEST_SLEEP)

            if all_dfs:
                full = pd.concat(all_dfs, ignore_index=True)
                save_parquet(full, sym, interval)
            else:
                logging.info(f"⚙️ No new data fetched for {sym}-{interval}")
            time.sleep(0.5)

if __name__ == "__main__":
    run_fetch()
