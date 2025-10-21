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

BINANCE_API_URL = "https://data-api.binance.com/api/v3/klines"
BINANCE_MAX_LIMIT = 1000
RETRY_LIMIT = 4
REQUEST_SLEEP = 0.15  # polite pause between requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# ==============================
# ✅ FETCH FROM BINANCE
# ==============================
def fetch_klines(symbol,
                 interval,
                 start_ts=None,
                 end_ts=None,
                 limit=BINANCE_MAX_LIMIT):
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
            logging.warning(
                f"[{symbol}-{interval}] Retry {attempt}/{RETRY_LIMIT} failed: {e}"
            )
            time.sleep(attempt * 1.5)
    logging.error(f"[{symbol}-{interval}] Failed after {RETRY_LIMIT} retries.")
    return []


# ==============================
# ✅ CONVERT TO DATAFRAME
# ==============================
def to_df(klines, symbol, interval):
    if not klines:
        return pd.DataFrame()
    df = pd.DataFrame(klines,
                      columns=[
                          "open_time", "open", "high", "low", "close",
                          "volume", "close_time", "quote_asset_volume",
                          "num_trades", "taker_buy_base", "taker_buy_quote",
                          "ignore"
                      ])
    df["symbol"] = symbol
    df["interval"] = interval
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    df[["open", "high", "low", "close",
        "volume"]] = df[["open", "high", "low", "close",
                         "volume"]].astype(float)
    df["num_trades"] = df["num_trades"].astype(int)
    df = df[[
        "symbol", "interval", "open_time", "close_time", "open", "high", "low",
        "close", "volume", "num_trades"
    ]]
    return df


# ==============================
# ✅ SAVE TO PARQUET (MERGE)
# ==============================
def save_parquet(df, symbol, interval):
    path = OUT_DIR / f"{symbol}_{interval}.parquet"
    if path.exists():
        try:
            old = pd.read_parquet(path)
            combined = pd.concat([old, df], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["symbol", "interval", "open_time"])
            combined = combined.sort_values("open_time")
        except Exception as e:
            logging.warning(f"Failed to merge with existing {path}: {e}")
            combined = df
    else:
        combined = df
    combined.to_parquet(path, index=False)
    logging.info(f"💾 Saved {len(combined)} rows → {path}")


# ==============================
# ✅ MAIN LOOP
# ==============================
def run_fetch(days_back=1):
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days_back)

    for sym in SYMBOLS:
        for interval in TIMEFRAMES:
            logging.info(
                f"📥 Fetching {sym} {interval} from {start_time.date()} to now..."
            )
            current = start_time
            all_dfs = []

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
            time.sleep(0.5)


if __name__ == "__main__":
    run_fetch(days_back=1)
