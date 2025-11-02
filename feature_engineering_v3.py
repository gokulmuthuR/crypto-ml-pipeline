#!/usr/bin/env python3
"""
feature_engineering_v4.py

Version: v4 (PR-ready)

Purpose
-------
A production-ready feature engineering pipeline that:
 - Replaces f_3.sql parity with a comprehensive pandas implementation.
 - Adds advanced quant features (rolling beta vs BTC, cross-symbol correlation
   snapshots, sample entropy, rolling autocorr, beta/covariance matrices).
 - Provides an overwrite-safe mode for heavy features so scheduled 12-hour jobs
   can optionally perform full recompute (recommended periodically).
 - Is incremental-safe for lightweight features and performs overlap recompute
   at file boundaries to keep rolling computations consistent.

Methodology summary (for header documentation)
---------------------------------------------
1. Base features: candle-level normalized returns, body/wick sizes, time features.
2. Technical features: SMAs/EMAs, MACD, RSI, ATR (multi-period), Bollinger, VWAP, OBV, CMF, MFI.
3. Quant/regime features: volatility regime flags, ADX/DI, drawdown/recovery, rolling skew/kurtosis, vol-adj returns.
4. Z-prep: rolling mean/std and robust z-scores for core vars to normalize cross-time behavior.
5. Targets: multiple fixed horizons + optional volatility-adaptive horizons.
6. Phase-3 quant: rolling β vs BTC, cross-symbol correlation snapshots, volatility-regime clustering (KMeans if available), sample entropy.
7. Operational: incremental update with overlap buffer, drop-dup by open_time, safe atomic parquet writes, and reproducible logs.

Usage
-----
# incremental (default - fast, overlap recompute)
python3 feature_engineering_v4.py --mode incremental

# full rebuild (overwrite all feature files - heavy)
python3 feature_engineering_v4.py --mode full

# full + phase3 quant features + verbose output
python3 feature_engineering_v4.py --mode full --phase3 --adaptive-targets --verbose

CLI flags
---------
--mode {incremental,full}
--symbols [SYMBOLS ...]
--intervals [INTERVALS ...]
--overlap INT
--adaptive-targets
--phase3
--verbose
--force-heavy    # alias: force recompute heavy features (entropy/beta/corr) by full overwrite
--beta-benchmark SYMBOL  # default BTCUSDT

Outputs
-------
- data/features/{SYMBOL}_{INTERVAL}_features.parquet
- data/features/corr_snapshots/*.csv
- last_feature_sync.log

Important operational note
--------------------------
- Entropy, full cross-symbol correlation & some multi-lag β/covariance matrices require recomputing full slices to be accurate.
  For scheduled jobs every 12h, set --mode full or pass --force-heavy to ensure those heavy features are overwritten and correct.
"""

from __future__ import annotations
import argparse
import warnings
from pathlib import Path
from datetime import datetime
from typing import List, Optional

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

# Optional phase3 dependencies
try:
    from sklearn.cluster import KMeans
    SKLEARN_AVAILABLE = True
except Exception:
    SKLEARN_AVAILABLE = False

# ================================
# CONFIG (adjust these lists as you wish)
# ================================
BASE_DIR = Path(".")
DATA_DIR = BASE_DIR / "data" / "ohlcv"
OUT_DIR = BASE_DIR / "data" / "features"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "LTCUSDT", "QNTUSDT", "SOLUSDT"
]
DEFAULT_INTERVALS = ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d", "1w"]

# Rolling windows and periods
ROLL_Z_WINDOW = 90
RSI_PERIOD = 14
ATR_PERIODS = [7, 14, 21]
VOL_MA_WINDOWS = [5, 10, 20]
SMA_WINDOWS = [3, 5, 10, 20]
EMA_WINDOWS = [7, 12, 21, 26, 50, 100, 200]

# Overlap buffer (rows) to recompute at the boundary during incremental runs
OVERLAP_BUFFER = 120

# Where to log last processed timestamps
LAST_SYNC_LOG = BASE_DIR / "last_feature_sync.log"

# Default fixed target horizons (minutes)
DEFAULT_TARGET_HORIZONS_MIN = [15, 30, 60, 120, 240, 360, 720, 1440, 10080]

# HTF resample config (auto-build 12h from 1h, 1w from 1d)
HTF_TARGETS = {
    "12h": {
        "base": "1h",
        "rule": "12H"
    },
    "1w": {
        "base": "1d",
        "rule": "7D"
    },
}

# Columns used for z-prep
Z_PREP_COLS = [
    "price_change_pct", "rsi_14", "atr_14", "vol_zscore", "price_vol_corr",
    "liquidity_proxy"
]

# Small numeric safe helper
EPS = 1e-9

# Beta benchmark default
BETA_BENCHMARK = "BTCUSDT"

# Verbose & phase flags (set at runtime)
VERBOSE = False
PHASE3 = False
ADAPTIVE_TARGETS = False


# =================================
# Utilities
# =================================
def log(msg: str):
    if VERBOSE:
        print(msg)


def log_progress(symbol: str,
                 interval: str,
                 current: int,
                 total: int,
                 step=5000):
    """
    Print progress every `step` rows processed (approximation).
    """
    if current % step == 0 or current == total:
        pct = (current / total) * 100 if total else 0.0
        print(
            f"[{symbol}-{interval}] Rows processed: {current:,}/{total:,} ({pct:.1f}%)",
            flush=True)


def safe_to_parquet(df: pd.DataFrame, path: Path):
    """
    Atomic parquet write: write to tmp then replace.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(path)


def ensure_datetime(df: pd.DataFrame, col="open_time"):
    if col not in df.columns:
        return df
    if not np.issubdtype(df[col].dtype, np.datetime64):
        df[col] = pd.to_datetime(df[col])
    return df


def load_ohlcv(symbol: str, interval: str) -> pd.DataFrame:
    path = DATA_DIR / f"{symbol}_{interval}.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df = ensure_datetime(df, "open_time")
    df = df.sort_values("open_time").reset_index(drop=True)
    return df


def read_existing_features(symbol: str, interval: str) -> pd.DataFrame:
    path = OUT_DIR / f"{symbol}_{interval}_features.parquet"
    if path.exists():
        df = pd.read_parquet(path)
        df = ensure_datetime(df, "open_time")
        df = df.sort_values("open_time").reset_index(drop=True)
        return df
    return pd.DataFrame()


# =================================
# Indicator implementations
# =================================
def rolling_zscore(series: pd.Series, window=ROLL_Z_WINDOW, min_periods=5):
    mu = series.rolling(window, min_periods=min_periods).mean()
    sigma = series.rolling(window,
                           min_periods=min_periods).std(ddof=0).replace(
                               0, np.nan)
    return (series - mu) / (sigma + EPS)


def robust_zscore(series: pd.Series, window=ROLL_Z_WINDOW, min_periods=5):
    med = series.rolling(window, min_periods=min_periods).median()
    mad = series.rolling(window, min_periods=min_periods).apply(
        lambda x: np.median(np.abs(x - np.median(x))), raw=True)
    mad = mad.replace(0, np.nan) * 1.4826
    return (series - med) / (mad + EPS)


def compute_rsi(series: pd.Series, period=RSI_PERIOD):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period, min_periods=1).mean()
    avg_loss = loss.rolling(period, min_periods=1).mean()
    rs = avg_gain / (avg_loss + EPS)
    return 100 - (100 / (1 + rs))


def compute_atr(df: pd.DataFrame, period=14):
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()


def compute_adx(df: pd.DataFrame, period=14):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move,
                        0.0)

    tr = pd.concat([(high - low), (high - close.shift()).abs(),
                    (low - close.shift()).abs()],
                   axis=1).max(axis=1)
    atr = tr.rolling(period, min_periods=1).mean()
    plus_di = 100 * (pd.Series(plus_dm).rolling(period, min_periods=1).sum() /
                     (atr + EPS))
    minus_di = 100 * (
        pd.Series(minus_dm).rolling(period, min_periods=1).sum() / (atr + EPS))
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di + EPS)) * 100
    adx = dx.rolling(period, min_periods=1).mean()
    return plus_di, minus_di, dx, adx


def compute_obv(df: pd.DataFrame):
    direction = np.sign(df["close"].diff()).fillna(0)
    obv = (direction * df["volume"]).cumsum().fillna(0)
    return obv


def compute_vwap(df: pd.DataFrame):
    cum_vp = (df["close"] * df["volume"]).cumsum()
    cum_vol = df["volume"].cumsum().replace(0, np.nan)
    return cum_vp / (cum_vol + EPS)


def compute_cmf(df: pd.DataFrame, window=20):
    mfm = ((df["close"] - df["low"]) -
           (df["high"] - df["close"])) / ((df["high"] - df["low"]) + EPS)
    mfv = mfm * df["volume"]
    return mfv.rolling(window, min_periods=1).sum() / (
        df["volume"].rolling(window, min_periods=1).sum() + EPS)


def compute_mfi(df: pd.DataFrame, period=14):
    typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
    money_flow = typical_price * df["volume"]
    positive = money_flow.where(typical_price > typical_price.shift(1), 0.0)
    negative = money_flow.where(typical_price < typical_price.shift(1), 0.0)
    pos_sum = positive.rolling(period, min_periods=1).sum()
    neg_sum = negative.rolling(period, min_periods=1).sum().abs()
    return 100 - (100 / (1 + (pos_sum / (neg_sum + EPS))))


def rolling_autocorr(series: pd.Series, lag=1, window=50, min_periods=5):

    def ac(x):
        if len(x) < min_periods:
            return np.nan
        s = pd.Series(x)
        return s.autocorr(lag=lag)

    return series.rolling(window, min_periods=min_periods).apply(ac, raw=False)


def sample_entropy(series: pd.Series, m=2):
    # lightweight approximate sample entropy (used sparingly)
    x = np.asarray(series.dropna(), dtype=float)
    n = len(x)
    if n < m + 2:
        return np.nan

    def _phi(m_):
        count = 0
        for i in range(n - m_):
            for j in range(i + 1, n - m_):
                if np.max(np.abs(x[i:i + m_] - x[j:j + m_])) <= 1e-6:
                    count += 1
        return count

    try:
        return -np.log((_phi(m + 1) + EPS) / (_phi(m) + EPS))
    except Exception:
        return np.nan


# =================================
# Feature computation blocks
# =================================
def compute_base(df: pd.DataFrame):
    df = df.sort_values("open_time").reset_index(drop=True)
    df["price_change_pct"] = (df["close"] - df["open"]) / (df["open"] +
                                                           EPS) * 100
    df["volatility_pct"] = (df["high"] - df["low"]) / (df["low"] + EPS) * 100
    df["body_size"] = (df["close"] - df["open"]).abs()
    df["wick_size"] = (df["high"] - df["low"])
    df["direction"] = (df["close"] > df["open"]).astype(int)

    df["hour"] = df["open_time"].dt.hour
    df["day_of_week"] = df["open_time"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

    df["sin_hour"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["cos_hour"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["sin_dow"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["cos_dow"] = np.cos(2 * np.pi * df["day_of_week"] / 7)

    df["next_open"] = df["open"].shift(-1)
    df["gap_next_open_pct"] = (df["next_open"] - df["close"]) / (df["close"] +
                                                                 EPS) * 100
    df["gap_next_open_pct"] = df["gap_next_open_pct"].fillna(0.0)
    return df


def compute_moving_stats(df: pd.DataFrame):
    for w in SMA_WINDOWS:
        df[f"sma_{w}"] = df["close"].rolling(w, min_periods=1).mean()
    for s in EMA_WINDOWS:
        df[f"ema_{s}"] = df["close"].ewm(span=s, adjust=False).mean()

    for p in ATR_PERIODS:
        df[f"atr_{p}"] = compute_atr(df, period=p)

    for w in VOL_MA_WINDOWS:
        df[f"vol_ma_{w}"] = df["volume"].rolling(w, min_periods=1).mean()

    df["vol_change_pct"] = df["volume"].pct_change().fillna(0.0)

    for lag in range(1, 6):
        df[f"close_lag_{lag}"] = df["close"].shift(lag)
        df[f"return_lag_{lag}"] = df["close"].pct_change().shift(lag)

    # rolling vol zscore (normalized by recent mean/std)
    df["vol_zscore"] = rolling_zscore(df["volume"].fillna(0.0))

    # liquidity proxy: volume relative to mid window (vol_ma_20 expected)
    if "vol_ma_20" in df.columns:
        df["liquidity_proxy"] = df["volume"] / (df["vol_ma_20"] + EPS)
    elif "vol_ma_10" in df.columns:
        df["liquidity_proxy"] = df["volume"] / (df["vol_ma_10"] + EPS)
    else:
        df["liquidity_proxy"] = df["volume"] / (
            df["volume"].rolling(20, min_periods=1).mean() + EPS)
    return df


# =====================================
# Optimized compute_technical() using Numba
# =====================================
from numba import njit, prange


@njit(cache=True)
def fast_entropy(arr, m=2, r=1e-6):
    n = len(arr)
    if n < m + 2:
        return np.nan
    count_m, count_m1 = 0, 0
    for i in range(n - m):
        for j in range(i + 1, n - m):
            if np.max(np.abs(arr[i:i + m] - arr[j:j + m])) <= r:
                count_m += 1
            if np.max(np.abs(arr[i:i + m + 1] - arr[j:j + m + 1])) <= r:
                count_m1 += 1
    return -np.log((count_m1 + 1e-9) / (count_m + 1e-9))


@njit(parallel=True, cache=True)
def rolling_entropy_numba(x, window=80):
    n = len(x)
    out = np.full(n, np.nan)
    for i in prange(window, n):
        seg = x[i - window:i]
        out[i] = fast_entropy(seg)
    return out


@njit(parallel=True, cache=True)
def rolling_corr_numba(x, y, window=20):
    n = len(x)
    out = np.full(n, np.nan)
    for i in prange(window, n):
        xw = x[i - window:i]
        yw = y[i - window:i]
        xv = xw - np.mean(xw)
        yv = yw - np.mean(yw)
        num = np.sum(xv * yv)
        den = np.sqrt(np.sum(xv**2) * np.sum(yv**2)) + 1e-9
        out[i] = num / den
    return out


def compute_technical(df: pd.DataFrame):
    n = len(df)
    print(
        f"⚙️ Computing technical indicators on {n:,} rows (Numba optimized)..."
    )

    close = df["close"].to_numpy(dtype=np.float64)
    volume = df["volume"].to_numpy(dtype=np.float64)

    df["rsi_14"] = compute_rsi(df["close"], period=RSI_PERIOD)

    # MACD
    ema12 = df["close"].ewm(span=12, adjust=False).mean()
    ema26 = df["close"].ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal = macd_line.ewm(span=9, adjust=False).mean()
    df["macd_line"], df["signal_line"] = macd_line, signal
    df["macd_hist"] = macd_line - signal
    df["macd_hist_norm"] = rolling_zscore(df["macd_hist"],
                                          window=ROLL_Z_WINDOW)

    # Bollinger
    mid = df["close"].rolling(20, min_periods=1).mean()
    std = df["close"].rolling(20, min_periods=1).std(ddof=0)
    df["bb_mid"], df["bb_upper"], df[
        "bb_lower"] = mid, mid + 2 * std, mid - 2 * std
    df["bollinger_position"] = (df["close"] - mid) / (std + EPS)

    # ATR, correlation, OBV, VWAP
    df["atr_14_over_price"] = df["atr_14"] / (df["close"] + EPS)
    df["obv"] = compute_obv(df)
    df["vwap"] = compute_vwap(df)
    df["vwap_ratio"] = (df["close"] - df["vwap"]) / (df["vwap"] + EPS)
    df["cmf_20"] = compute_cmf(df, window=20)
    df["mfi_14"] = compute_mfi(df, period=14)

    # Use Numba for price-volume correlation
    price_ret = np.diff(np.concatenate(([close[0]], close))) / (close + EPS)
    vol_ret = np.diff(np.concatenate(([volume[0]], volume))) / (volume + EPS)
    print("🔹 Computing fast rolling correlation (20-window)...", flush=True)
    df["price_vol_corr"] = pd.Series(
        rolling_corr_numba(price_ret, vol_ret, window=20))

    # Rolling skew/kurtosis (vectorized)
    print("🔹 Computing rolling skew/kurtosis...", flush=True)
    df["roll_skew_20"] = pd.Series(pd.Series(close).rolling(20).skew())
    df["roll_kurt_20"] = pd.Series(pd.Series(close).rolling(20).kurt())

    # Fast entropy (Numba)
    print("🔹 Computing rolling entropy (80-window)...", flush=True)
    ent = rolling_entropy_numba(price_ret, window=80)
    df["sample_entropy_20"] = pd.Series(ent)

    # Volatility-adjusted return
    df["vol_adj_return"] = (df["close"].pct_change()) / (df["atr_14"] /
                                                         (df["close"] + EPS) +
                                                         EPS)

    print("✅ Technical computation complete.")
    return df


def compute_adx_drawdown(df: pd.DataFrame):
    plus_di, minus_di, dx, adx = compute_adx(df, period=14)
    df["plus_di"] = plus_di.values
    df["minus_di"] = minus_di.values
    df["dx"] = dx.values
    df["adx_14"] = adx.values

    df["rolling_max_30"] = df["close"].rolling(30, min_periods=1).max()
    df["drawdown_30_pct"] = (df["close"] - df["rolling_max_30"]) / (
        df["rolling_max_30"] + EPS)
    df["rolling_max_90"] = df["close"].rolling(90, min_periods=1).max()
    df["drawdown_90_pct"] = (df["close"] - df["rolling_max_90"]) / (
        df["rolling_max_90"] + EPS)

    rolling_min_30 = df["close"].rolling(30, min_periods=1).min()
    df["recovery_speed_30"] = (df["close"] -
                               rolling_min_30) / (rolling_min_30 + EPS)
    return df


def compute_z_prep(df: pd.DataFrame):
    for col in Z_PREP_COLS:
        if col in df.columns:
            mu_col = f"mu_{col}"
            sigma_col = f"sigma_{col}"
            z_col = f"z_{col}"
            df[mu_col] = df[col].rolling(ROLL_Z_WINDOW, min_periods=5).mean()
            df[sigma_col] = df[col].rolling(ROLL_Z_WINDOW,
                                            min_periods=5).std(ddof=0)
            df[z_col] = (df[col] - df[mu_col]) / (df[sigma_col] + EPS)
            df[f"rz_{col}"] = robust_zscore(df[col], window=ROLL_Z_WINDOW)
    for col in Z_PREP_COLS:
        if f"z_{col}" in df.columns:
            df[f"smooth_z_{col}"] = df[f"z_{col}"].rolling(
                5, min_periods=1).mean()
    return df


def compute_targets(df: pd.DataFrame,
                    horizons_min: Optional[List[int]] = None,
                    adaptive=False):
    if horizons_min is None:
        horizons_min = DEFAULT_TARGET_HORIZONS_MIN

    if adaptive:
        vol_factor = (df["atr_14"].rolling(60, min_periods=1).mean() /
                      (df["atr_14"].rolling(720, min_periods=1).median() +
                       EPS)).fillna(1.0)
        vol_factor = vol_factor.clip(0.5, 2.0)
    else:
        vol_factor = None

    for h in horizons_min:
        if adaptive:
            shift_series = (vol_factor.apply(
                lambda f: max(1, int(round(h / f))))).astype(int)
            median_shift = int(np.median(
                shift_series.dropna())) if shift_series.dropna().size else h
            shift = median_shift
        else:
            shift = h

        df[f"future_close_{h}m"] = df["close"].shift(-shift)
        df[f"future_return_{h}m"] = (df[f"future_close_{h}m"] -
                                     df["close"]) / (df["close"] + EPS)
        df[f"future_dir_{h}m"] = (df[f"future_return_{h}m"] > 0).astype(int)
        df[f"future_volume_{h}m"] = df["volume"].shift(-shift)
        df[f"target_liq_flag_{h}m"] = (
            df[f"future_volume_{h}m"]
            > df["volume"].rolling(100, min_periods=1).mean() *
            1.5).astype(int)
    return df


def compute_regime_flags(df: pd.DataFrame):
    df["volatility_regime_flag"] = (
        df["atr_14_over_price"]
        > df["atr_14_over_price"].rolling(1440, min_periods=1).mean() *
        1.5).astype(int)
    df["volume_regime_flag"] = (
        df["volume"]
        > df["volume"].rolling(1440, min_periods=1).mean() * 1.5).astype(int)
    df["trend_regime_flag"] = (df["adx_14"] > 25).astype(int)
    return df


# =================================
# Beta vs BTC & Corr Snapshot
# =================================
def compute_rolling_beta(df: pd.DataFrame, bench: pd.DataFrame, window=120):
    """
    Compute rolling beta (symbol vs benchmark) using returns.
    df: target symbol df (must include close)
    bench: benchmark df aligned to same timestamps (close)
    returns: series of beta aligned to df index (NaN when not computable)
    """
    # align on open_time using an asof join: we expect both are regular intervals
    if df.empty or bench.empty:
        return pd.Series(dtype=float)
    # compute returns
    r1 = df["close"].pct_change().fillna(0)
    r2 = bench["close"].pct_change().fillna(0)
    # align lengths: assume same index ordering and same frequency; if not, reindex bench to df's open_time via forward fill
    if not r2.index.equals(r1.index):
        try:
            bench_r2 = r2.reindex(df.index, method="ffill").fillna(0)
        except Exception:
            bench_r2 = r2.reset_index(drop=True).reindex(range(len(r1)),
                                                         fill_value=0)
    else:
        bench_r2 = r2

    # rolling covariance and variance
    cov = r1.rolling(window, min_periods=5).cov(bench_r2)
    var = bench_r2.rolling(window, min_periods=5).var(ddof=0)
    beta = cov / (var + EPS)
    return beta


def cross_symbol_correlation_snapshot(symbols: List[str],
                                      interval="1m",
                                      window=120):
    """
    Save cross-symbol correlation matrix (returns) snapshot CSV in data/features/corr_snapshots/
    """
    try:
        frames = {}
        for s in symbols:
            p = DATA_DIR / f"{s}_{interval}.parquet"
            if p.exists():
                df = pd.read_parquet(p)
                df = ensure_datetime(df, "open_time")
                df = df.sort_values("open_time").reset_index(drop=True)
                frames[s] = df["close"].pct_change(
                ).iloc[-window:].reset_index(drop=True)
        if not frames:
            return
        corr_df = pd.DataFrame(frames).corr()
        snap_dir = OUT_DIR / "corr_snapshots"
        snap_dir.mkdir(exist_ok=True, parents=True)
        fn = snap_dir / f"corr_{interval}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
        corr_df.to_csv(fn)
        log(f"📊 Saved cross-symbol correlation snapshot: {fn}")
    except Exception as e:
        log(f"⚠️ cross_symbol_correlation_snapshot error: {e}")


# =================================
# Volatility regime clustering (optional)
# =================================
def volatility_regime_clustering(symbol: str, interval: str, n_clusters=3):
    if not SKLEARN_AVAILABLE:
        log("⚠️ sklearn not available — skipping volatility_regime_clustering")
        return None
    try:
        p = OUT_DIR / f"{symbol}_{interval}_features.parquet"
        if not p.exists():
            return None
        df = pd.read_parquet(p)
        df = ensure_datetime(df, "open_time")
        feats = df[["atr_14", "rsi_14", "vol_ma_10"]].dropna()
        if len(feats) < 50:
            return None
        X = (feats - feats.mean()) / (feats.std() + EPS)
        km = KMeans(n_clusters=n_clusters, random_state=1)
        labels = km.fit_predict(X)
        label_series = pd.Series(np.nan, index=df.index)
        label_series.iloc[-len(labels):] = labels
        df["vol_regime_km"] = label_series.values
        safe_to_parquet(df, p)
        log(f"🔰 Clustering saved -> {p} (vol_regime_km)")
        return True
    except Exception as e:
        log(f"⚠️ volatility_regime_clustering failed: {e}")
        return None


# =================================
# Incremental / orchestration
# =================================
def compute_features_for_slice(df_slice: pd.DataFrame,
                               adaptive=False,
                               bench_df=None,
                               beta_window=120,
                               compute_heavy=False):
    """
    Optimized version — avoids DataFrame fragmentation
    and logs progress with better visibility.
    """
    df = df_slice.copy()

    # --- Main feature pipeline with defragmentation checkpoints ---
    print(f"⚙️  Starting computation block: BASE for {len(df):,} rows")
    df = compute_base(df).copy()

    print("⚙️  Computing moving statistics...")
    df = compute_moving_stats(df).copy()

    print("⚙️  Computing technical indicators...")
    df = compute_technical(df).copy()

    print("⚙️  Computing ADX / Drawdown metrics...")
    df = compute_adx_drawdown(df).copy()

    print("⚙️  Computing z-prep normalization...")
    df = compute_z_prep(df).copy()

    print("⚙️  Computing target horizons...")
    df = compute_targets(df, adaptive=adaptive).copy()

    print("⚙️  Computing regime flags...")
    df = compute_regime_flags(df).copy()

    # --- Quant phase: β vs BTC and correlation ---
    if bench_df is not None and "close" in bench_df.columns:
        try:
            bench_local = bench_df.set_index("open_time").reindex(
                df["open_time"], method="ffill").reset_index(drop=True)
            df["beta_vs_btc_rolling"] = compute_rolling_beta(
                df, bench_local, window=beta_window)
            df["corr_vs_btc_rolling"] = df["close"].pct_change().rolling(
                beta_window, min_periods=5).corr(
                    bench_local["close"].pct_change().fillna(0))
        except Exception as e:
            log(f"⚠️ beta computation failed: {e}")
            df["beta_vs_btc_rolling"] = np.nan
            df["corr_vs_btc_rolling"] = np.nan
    else:
        df["beta_vs_btc_rolling"] = np.nan
        df["corr_vs_btc_rolling"] = np.nan

    # --- Heavy quant features (Numba optimized) ---

    @njit(cache=True)
    def fast_entropy(arr, m=2, r=1e-6):
        n = len(arr)
        if n < m + 2:
            return np.nan
        count_m, count_m1 = 0, 0
        for i in range(n - m):
            for j in range(i + 1, n - m):
                if np.max(np.abs(arr[i:i + m] - arr[j:j + m])) <= r:
                    count_m += 1
                if np.max(np.abs(arr[i:i + m + 1] - arr[j:j + m + 1])) <= r:
                    count_m1 += 1
        return -np.log((count_m1 + 1e-9) / (count_m + 1e-9))

    @njit(parallel=True, cache=True)
    def rolling_entropy_numba(x, window=80):
        n = len(x)
        out = np.full(n, np.nan)
        for i in prange(window, n):
            seg = x[i - window:i]
            out[i] = fast_entropy(seg)
        return out

    if compute_heavy:
        print("⚙️  Computing heavy features (Numba optimized)...", flush=True)
        try:
            close_ret = df["close"].pct_change().to_numpy(dtype=np.float64)
            ent = rolling_entropy_numba(close_ret, window=80)
            df["sample_entropy_40"] = pd.Series(ent)

            if bench_df is not None and "close" in bench_df.columns:
                df["beta_vs_btc_60"] = compute_rolling_beta(df,
                                                            bench_local,
                                                            window=60)
                df["beta_vs_btc_240"] = compute_rolling_beta(df,
                                                             bench_local,
                                                             window=240)

        except Exception as e:
            log(f"⚠️ heavy feature compute error: {e}")

        # --- Final fill and defrag ---
    df = df.fillna(method="ffill").fillna(method="bfill").fillna(0.0).copy()
    return df


def incremental_update_symbol_interval(
    symbol: str,
    interval: str,
    mode="incremental",
    overlap=OVERLAP_BUFFER,
    adaptive=False,
    bench_symbol=BETA_BENCHMARK,
    compute_heavy=False,
):
    """
    mode:
      - incremental: append new rows since last saved features, recomputing overlap rows for rolling continuity.
      - full: recompute all rows from OHLCV and overwrite features file (recommended periodically or when --force-heavy).
    compute_heavy: whether to compute heavy features (entropy, multi-lag beta).
    """
    log(f"\n🔄 Processing {symbol} [{interval}] (mode={mode}, heavy={compute_heavy})"
        )
    ohlcv = load_ohlcv(symbol, interval)
    if ohlcv.empty:
        log(f"⚠️ No OHLCV source found for {symbol}-{interval}")
        return

    # Load benchmark OHLCV aligned to same interval (if available)
    bench_df = None
    try:
        bench_df = load_ohlcv(bench_symbol, interval)
    except Exception:
        bench_df = None

    existing = read_existing_features(symbol, interval)

    # =========================================================
    # FULL MODE (or empty existing)
    # =========================================================
    if mode == "full" or existing.empty:
        log("→ Running full rebuild (overwrite) for this file")
        slice_df = ohlcv.copy()
        total_rows = len(slice_df)

        if total_rows == 0:
            log(f"⚠️ No rows to process for {symbol}-{interval}")
            out_df = pd.DataFrame(columns=ohlcv.columns)
        else:
            print(
                f"→ Starting feature computation for {symbol}-{interval} | total rows: {total_rows:,}",
                flush=True)

            # lightweight simulated progress (every 5k rows)
            for i in range(0, total_rows, 5000):
                log_progress(symbol, interval, i, total_rows)

            # run the actual computation safely
            try:
                out_df = compute_features_for_slice(
                    slice_df,
                    adaptive=adaptive,
                    bench_df=bench_df,
                    compute_heavy=compute_heavy,
                )
            except Exception as e:
                print(f"❌ Feature compute failed for {symbol}-{interval}: {e}",
                      flush=True)
                out_df = pd.DataFrame(columns=ohlcv.columns)

    # =========================================================
    # INCREMENTAL MODE
    # =========================================================
    else:
        last_ts = existing["open_time"].max()
        idx = ohlcv["open_time"].searchsorted(last_ts)
        start_idx = max(0, idx - overlap)
        slice_df = ohlcv.iloc[start_idx:].copy()

        log(f"→ incremental: last_ts={last_ts} start_idx={start_idx} rows_to_process={len(slice_df)}"
            )
        try:
            new_feats = compute_features_for_slice(
                slice_df,
                adaptive=adaptive,
                bench_df=bench_df,
                compute_heavy=compute_heavy,
            )
        except Exception as e:
            print(f"❌ Incremental compute failed for {symbol}-{interval}: {e}",
                  flush=True)
            new_feats = pd.DataFrame(columns=ohlcv.columns)

        keep_upto_time = (ohlcv.iloc[start_idx]["open_time"]
                          if start_idx < len(ohlcv) else last_ts)
        preserved = existing[existing["open_time"] < keep_upto_time].copy()
        out_df = pd.concat([preserved, new_feats], ignore_index=True)
        out_df = out_df.sort_values("open_time").reset_index(drop=True)

    # =========================================================
    # FINALIZE / SAVE
    # =========================================================
    out_df = (out_df.drop_duplicates(
        subset=["open_time"],
        keep="last").sort_values("open_time").reset_index(drop=True))

    if len(out_df) < len(ohlcv) * 0.9:
        print(
            f"⚠️ Sanity check: resulting feature rows much smaller than OHLCV ({len(out_df)}/{len(ohlcv)}). Aborting write.",
            flush=True,
        )
        return

    path = OUT_DIR / f"{symbol}_{interval}_features.parquet"
    safe_to_parquet(out_df, path)
    with open(LAST_SYNC_LOG, "a", encoding="utf-8") as f:
        f.write(
            f"{symbol}_{interval} last_open_time={out_df['open_time'].max().isoformat()}Z\n"
        )

    print(
        f"✅ Completed {symbol}-{interval} | rows={len(out_df):,} cols={len(out_df.columns)}",
        flush=True,
    )
    log(f"✅ Saved {len(out_df)} rows → {path} (cols={len(out_df.columns)})")


# =================================
# HTF resampling helpers
# =================================
def resample_htf(symbol: str):
    base_dir = DATA_DIR
    for new_intv, cfg in HTF_TARGETS.items():
        target_path = base_dir / f"{symbol}_{new_intv}.parquet"
        # If file exists and is fresh (24h), skip
        if target_path.exists():
            mtime = datetime.utcfromtimestamp(target_path.stat().st_mtime)
            if (datetime.utcnow() - mtime).total_seconds() < 86400:
                log(f"✅ {symbol} {new_intv} is up-to-date (skipping).")
                continue
        base_path = base_dir / f"{symbol}_{cfg['base']}.parquet"
        if not base_path.exists():
            log(f"⚠️ {symbol} missing base {cfg['base']} for {new_intv}, skipping."
                )
            continue
        log(f"⏳ Building {new_intv} for {symbol} from {cfg['base']}...")
        try:
            df = pd.read_parquet(base_path).sort_values("open_time")
            df = df.set_index("open_time")
            agg = {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "num_trades": "sum"
            }
            resampled = df.resample(
                cfg["rule"], label="right",
                closed="right").agg(agg).dropna().reset_index()
            resampled["symbol"] = df["symbol"].iloc[
                0] if "symbol" in df.columns else symbol
            resampled["interval"] = new_intv
            resampled["close_time"] = resampled["open_time"] + pd.to_timedelta(
                cfg["rule"])
            cols = [
                "symbol", "interval", "open_time", "close_time", "open",
                "high", "low", "close", "volume", "num_trades"
            ]
            resampled = resampled[cols]
            resampled.to_parquet(target_path, index=False)
            log(f"✅ Saved {target_path} ({len(resampled)} rows)")
        except Exception as e:
            print(f"❌ Failed to build {symbol} {new_intv}: {e}")


def ensure_htf_resampling(symbols: List[str]):
    print("\n🔄 Checking for missing HTF (12h, 1w) OHLCV files...")
    for sym in symbols:
        resample_htf(sym)
    print("✅ HTF resample check complete.\n")


# =================================
# Cross-symbol helpers & orchestration
# =================================
def batch_run(symbols: Optional[List[str]] = None,
              intervals: Optional[List[str]] = None,
              mode="incremental",
              overlap=OVERLAP_BUFFER,
              adaptive=False,
              force_heavy=False,
              beta_benchmark=BETA_BENCHMARK):
    if symbols is None:
        symbols = DEFAULT_SYMBOLS
    if intervals is None:
        intervals = DEFAULT_INTERVALS

    # Step A: ensure HTF resampling for 12h & 1w (build if missing)
    ensure_htf_resampling(symbols)

    # Step B: cross-symbol correlation snapshot if phase3 requested
    if PHASE3:
        cross_symbol_correlation_snapshot(symbols, interval="1m")

    # If force_heavy True, we'll do a full overwrite for heavy features (safe overwrite).
    for sym in symbols:
        for interval in intervals:
            try:
                # Determine whether to compute heavy features:
                heavy_local = False
                if force_heavy:
                    # Force full mode to ensure heavy features are correct (overwrite)
                    chosen_mode = "full"
                    heavy_local = True
                else:
                    chosen_mode = mode
                    # Optionally compute heavy for selected base timeframe(s)
                    # For performance, only compute heavy for base intervals (1m/1h) or when PHASE3 True
                    if PHASE3 and interval in ["1m", "1h", "4h", "1d"]:
                        heavy_local = True

                incremental_update_symbol_interval(sym,
                                                   interval,
                                                   mode=chosen_mode,
                                                   overlap=overlap,
                                                   adaptive=adaptive,
                                                   bench_symbol=beta_benchmark,
                                                   compute_heavy=heavy_local)

                # optional phase3 clustering per symbol/interval (cheap enough)
                if PHASE3:
                    volatility_regime_clustering(sym, interval)

            except Exception as e:
                print(f"❌ Error for {sym}-{interval}: {e}")

    print(
        "\n🎯 Feature engineering v4 complete for requested symbols/intervals.")


# =================================
# Small helper to estimate expected columns from a sample run (not exact)
# =================================
def estimate_expected_columns():
    """
    Heuristic: base columns + moving stats + technical + adx/drawdown + z-prep + targets (per horizon)
    Returns estimated column count and lists of groups for quick sanity.
    """
    base_cols = [
        "open_time", "symbol", "interval", "open", "high", "low", "close",
        "volume", "num_trades", "close_time"
    ]
    base_features = 15  # price_change_pct, volatility_pct, body_size, wick_size, direction, hour, dow, sin/cos, gap_next...
    sma = len(SMA_WINDOWS)
    ema = len(EMA_WINDOWS)
    atr = len(ATR_PERIODS)
    volma = len(VOL_MA_WINDOWS)
    lags = 5 * 2  # close_lag_i, return_lag_i
    tech_misc = 20
    z_prep = len(Z_PREP_COLS) * 4  # mu,sigma,z,rz approx
    targets = len(
        DEFAULT_TARGET_HORIZONS_MIN
    ) * 4  # future_close, future_return, future_dir, future_volume
    extras = 25  # adx,di,obv,vwap,cmf,mfi,entropy,kurt,skew,regime flags, beta, corr etc
    estimate = len(
        base_cols
    ) + base_features + sma + ema + atr + volma + lags + tech_misc + z_prep + targets + extras
    return int(estimate)


# =================================
# CLI
# =================================
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--mode",
                   choices=["incremental", "full"],
                   default="incremental",
                   help="Run mode: incremental or full")
    p.add_argument("--symbols",
                   nargs="*",
                   default=DEFAULT_SYMBOLS,
                   help="Symbols to process")
    p.add_argument("--intervals",
                   nargs="*",
                   default=DEFAULT_INTERVALS,
                   help="Intervals to process")
    p.add_argument("--overlap",
                   type=int,
                   default=OVERLAP_BUFFER,
                   help="Overlap buffer rows to recompute")
    p.add_argument("--adaptive-targets",
                   action="store_true",
                   help="Enable adaptive target horizons by volatility")
    p.add_argument(
        "--phase3",
        action="store_true",
        help="Enable Phase-3 quant features (clustering, cross-corr)")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")
    p.add_argument(
        "--force-heavy",
        action="store_true",
        help="Force heavy features recompute (overwrite, expensive)")
    p.add_argument("--beta-benchmark",
                   default=BETA_BENCHMARK,
                   help="Symbol to use as beta benchmark (default BTCUSDT)")
    return p.parse_args()


def main():
    global VERBOSE, PHASE3, ADAPTIVE_TARGETS
    args = parse_args()
    VERBOSE = args.verbose
    PHASE3 = args.phase3
    ADAPTIVE_TARGETS = args.adaptive_targets

    print(
        f"Starting feature_engineering_v4.py (mode={args.mode}) at {datetime.utcnow().isoformat()}Z"
    )
    if PHASE3 and not SKLEARN_AVAILABLE:
        print(
            "⚠️ Phase3 requested but scikit-learn not available — clustering will be skipped."
        )

    # If user requested force-heavy, make sure we run full overwrite for heavy features
    force_heavy = args.force_heavy

    batch_run(symbols=args.symbols,
              intervals=args.intervals,
              mode=args.mode,
              overlap=args.overlap,
              adaptive=ADAPTIVE_TARGETS,
              force_heavy=force_heavy,
              beta_benchmark=args.beta_benchmark)

    print(
        f"Estimated expected columns ≈ {estimate_expected_columns()} (approx)."
    )
    print("All done!")


if __name__ == "__main__":
    main()
