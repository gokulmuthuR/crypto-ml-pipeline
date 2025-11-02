#!/usr/bin/env python3
"""
master_feature_merge_v6_12_enhanced_full.py

Full production-ready master merge script (streaming / memory-safe)
— preserves v6_11_full features and adds enhancement features for every peer,
using BTCUSDT as the market baseline.

Usage example:
python3 features/master_feature_merge_v6_12_enhanced_full.py \
  --mode full \
  --symbols BTCUSDT ETHUSDT BNBUSDT LTCUSDT QNTUSDT SOLUSDT \
  --intervals 1m 5m 15m 30m 1h 4h 12h 1d 1w \
  --compute-crosscorr --crosscorr-mode onthefly --row-chunk 50000 --verbose
"""
from __future__ import annotations
import argparse
import math
import warnings
import sys
import gc
import os
import time
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

# Optional modules
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW = True
except Exception:
    PYARROW = False

try:
    from numba import njit
    NUMBA = True
except Exception:
    NUMBA = False

try:
    import psutil
    PSUTIL = True
except Exception:
    PSUTIL = False

# -----------------------------
# Default config
# -----------------------------
BASE_DIR = Path(".")
FEATURES_DIR = BASE_DIR / "data" / "features"
OUT_DIR = BASE_DIR / "data" / "master_features"
TMP_DIR_NAME = "tmp_master_build"
TMP_DIR = OUT_DIR / TMP_DIR_NAME
OUT_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "LTCUSDT", "QNTUSDT", "SOLUSDT"
]
DEFAULT_INTERVALS = ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d", "1w"]

# Adaptive windows (short, medium, long)
CROSSCORR_WINDOWS = {
    "1m": [120, 240, 720],
    "5m": [48, 120, 288],
    "15m": [32, 96, 288],
    "30m": [24, 72, 168],
    "1h": [24, 72, 168],
    "4h": [24, 60, 180],
    "12h": [14, 36, 60],
    "1d": [14, 30, 60],
    "1w": [8, 15, 26],
}

EPS = 1e-9
VERBOSE = False
REFERENCE_SYMBOL = "BTCUSDT"  # BTC as market baseline

# -----------------------------
# Helpers: logging & memory
# -----------------------------
def log(msg: str):
    if VERBOSE:
        ts = datetime.utcnow().isoformat()
        print(f"[{ts}] {msg}", flush=True)


def human_bytes(n: float) -> str:
    if n is None:
        return "N/A"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:3.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PB"


def mem_status() -> str:
    if not PSUTIL:
        return "psutil not installed"
    m = psutil.virtual_memory()
    return f"mem_used={human_bytes(m.used)} mem_avail={human_bytes(m.available)} pct={m.percent}%"


# -----------------------------
# IO helpers
# -----------------------------
def ensure_datetime(df: pd.DataFrame, col: str = "open_time") -> pd.DataFrame:
    if col in df.columns and not np.issubdtype(df[col].dtype, np.datetime64):
        df[col] = pd.to_datetime(df[col])
    return df


def load_feature_file(symbol: str, interval: str) -> pd.DataFrame:
    path = FEATURES_DIR / f"{symbol}_{interval}_features.parquet"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df = ensure_datetime(df, "open_time")
    df = df.sort_values("open_time").reset_index(drop=True)
    return df


def safe_atomic_replace(tmp: Path, final: Path):
    # atomic replace: replace final with tmp
    try:
        tmp.replace(final)
    except Exception:
        # fallback: remove final then replace
        if final.exists():
            final.unlink()
        tmp.replace(final)


# -----------------------------
# Rolling corr utils (numba optional)
# -----------------------------
if NUMBA:

    @njit(cache=True)
    def _rolling_corr_numba(x: np.ndarray, y: np.ndarray, window: int, min_periods: int):
        n = x.shape[0]
        out = np.full(n, np.nan)
        if window <= 0:
            return out
        for i in range(n):
            start = i - window + 1
            if start < 0:
                continue
            sx = x[start:i + 1]
            sy = y[start:i + 1]
            mask = (~np.isnan(sx)) & (~np.isnan(sy))
            k = mask.sum()
            if k < min_periods:
                continue
            sxv = sx[mask]
            syv = sy[mask]
            mx = sxv.mean()
            my = syv.mean()
            cov = ((sxv - mx) * (syv - my)).sum() / k
            vx = ((sxv - mx)**2).sum() / k
            vy = ((syv - my)**2).sum() / k
            denom = math.sqrt(max(vx * vy, EPS))
            out[i] = cov / denom
        return out

    def rolling_corr(x: np.ndarray, y: np.ndarray, window: int, min_periods: int = 5):
        return _rolling_corr_numba(x.astype(np.float64), y.astype(np.float64), int(window), int(min_periods))
else:

    def rolling_corr(x: np.ndarray, y: np.ndarray, window: int, min_periods: int = 5):
        return pd.Series(x).rolling(window, min_periods=min_periods).corr(pd.Series(y)).to_numpy()


def rolling_diff(arr: np.ndarray) -> np.ndarray:
    out = np.full(len(arr), np.nan, dtype=np.float64)
    for i in range(1, len(arr)):
        a = arr[i]
        b = arr[i - 1]
        if np.isnan(a) or np.isnan(b):
            out[i] = np.nan
        else:
            out[i] = a - b
    return out


# -----------------------------
# Alignment + prefix helpers
# -----------------------------
def align_to_target_times(src_df: pd.DataFrame, target_times: pd.Series) -> pd.DataFrame:
    """pandas.merge_asof backward join to align src to target_times (last known)."""
    if src_df.empty:
        return pd.DataFrame({"open_time": target_times.values})
    left = pd.DataFrame({"open_time": pd.to_datetime(target_times.values)})
    src = src_df.sort_values("open_time").reset_index(drop=True)
    merged = pd.merge_asof(left, src, on="open_time", direction="backward")
    return merged


def prefix_cols_for_source(df: pd.DataFrame, prefix: str, symbol_suffix: Optional[str] = None, exclude: List[str] = ["open_time"]) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    mapping = {}
    for c in df.columns:
        if c in exclude:
            continue
        new = f"{prefix}_{c}"
        if symbol_suffix:
            new = f"{new}__{symbol_suffix}"
        mapping[c] = new
    if mapping:
        df = df.rename(columns=mapping)
    return df


# -----------------------------
# Enhancement Helpers
# -----------------------------
def compute_returns(df: pd.DataFrame, price_col="close"):
    if price_col not in df.columns:
        return pd.Series(np.nan, index=df.index)
    return df[price_col].astype(float).pct_change().fillna(0)


def compute_atr_percent(df: pd.DataFrame):
    # ATR% = (high - low) / close
    if not all(c in df.columns for c in ["high", "low", "close"]):
        return pd.Series(np.nan, index=df.index)
    return ((df["high"].astype(float) - df["low"].astype(float)) / (df["close"].astype(float) + EPS)).fillna(0)


def compute_rsi(series: pd.Series, period: int = 14):
    # classic Wilder RSI
    s = series.astype(float).fillna(method="ffill").fillna(0)
    delta = s.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/period, adjust=False).mean()
    ma_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = ma_up / (ma_down + EPS)
    rsi = 100 - (100 / (1 + rs))
    return rsi


def compute_macd_hist(series: pd.Series, fast=12, slow=26, signal=9):
    # returns macd_hist = MACD - signal
    if series.empty:
        return pd.Series(np.nan, index=series.index)
    fast_ema = series.ewm(span=fast, adjust=False).mean()
    slow_ema = series.ewm(span=slow, adjust=False).mean()
    macd = fast_ema - slow_ema
    macd_signal = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - macd_signal
    return hist


def compute_volume_zscore(series: pd.Series, window=30):
    s = series.astype(float).fillna(0)
    return (s - s.rolling(window).mean()) / (s.rolling(window).std() + EPS)


def compute_liq_imbalance(df: pd.DataFrame, window=20):
    # taker_buy_base / volume smoothed
    if not all(c in df.columns for c in ["taker_buy_base", "volume"]):
        return pd.Series(np.nan, index=df.index)
    ratio = df["taker_buy_base"].astype(float) / (df["volume"].astype(float) + EPS)
    return ratio.rolling(window).mean()


def compute_rolling_beta_and_resid(target_ret: pd.Series, ref_ret: pd.Series, window=50):
    # beta = cov(target,ref)/var(ref); residual volatility = rolling std of (target - beta*ref)
    t = target_ret.fillna(0)
    r = ref_ret.fillna(0)
    cov = t.rolling(window).cov(r)
    var = r.rolling(window).var()
    beta = cov / (var + EPS)
    residual = t - beta * r
    resid_vol = residual.rolling(window).std()
    return beta.fillna(np.nan), resid_vol.fillna(np.nan)


def compute_regime_shift(returns: pd.Series, short=30, long=90):
    zs_short = (returns - returns.rolling(short).mean()) / (returns.rolling(short).std() + EPS)
    zs_long = (returns - returns.rolling(long).mean()) / (returns.rolling(long).std() + EPS)
    return (zs_short - zs_long).fillna(0)


def compute_lagged_corr_delta(base_close: pd.Series, peer_close: pd.Series, short_window=24, long_window=72):
    # compute rolling corr difference: corr_short - corr_long (will be NaN where insufficient)
    corr_short = base_close.pct_change().rolling(short_window).corr(peer_close.pct_change())
    corr_long = base_close.pct_change().rolling(long_window).corr(peer_close.pct_change())
    return (corr_short - corr_long).fillna(np.nan)


def compute_momentum_confirm(rsi: pd.Series, macd_hist: pd.Series, vol_z: pd.Series):
    # simple integer score 0..3
    score = ((rsi > 50).astype(int) + (macd_hist > 0).astype(int) + (vol_z > 0).astype(int))
    return score


def add_enhancements_to_aligned(aligned_df: pd.DataFrame, btc_aligned: Optional[pd.DataFrame] = None):
    """
    Given an aligned DataFrame (aligned to target times), add enhancement columns in-place
    and return that DataFrame. These columns are then prefixed and appended in chunk builder.
    """
    if aligned_df.empty:
        return aligned_df

    df = aligned_df.copy()
    # returns & ATR%
    df["returns"] = compute_returns(df, price_col="close")
    df["atr_pct"] = compute_atr_percent(df)

    # RSI / MACD hist (based on close)
    if "close" in df.columns:
        df["rsi_14"] = compute_rsi(df["close"], period=14)
        df["macd_hist"] = compute_macd_hist(df["close"])
    else:
        df["rsi_14"] = np.nan
        df["macd_hist"] = np.nan

    # volume zscore
    if "volume" in df.columns:
        df["volume_zscore"] = compute_volume_zscore(df["volume"], window=30)
    else:
        df["volume_zscore"] = np.nan

    # liquidity imbalance
    df["liq_imbalance"] = compute_liq_imbalance(df, window=20)

    # momentum confirm
    df["momentum_confirm"] = compute_momentum_confirm(df["rsi_14"], df["macd_hist"], df["volume_zscore"])

    # compare vs BTC
    if btc_aligned is not None and not btc_aligned.empty:
        # ensure btc has returns and atr_pct precomputed
        btc_ret = btc_aligned.get("returns")
        btc_atr = btc_aligned.get("atr_pct")
        if btc_ret is None:
            btc_ret = compute_returns(btc_aligned, price_col="close")
        if btc_atr is None:
            btc_atr = compute_atr_percent(btc_aligned)
        # vol ratio vs BTC
        df["vol_ratio_vs_btc"] = df["atr_pct"] / (btc_atr + EPS)
        # rolling beta & resid vol
        beta, resid_vol = compute_rolling_beta_and_resid(df["returns"], btc_ret, window=50)
        df["rolling_beta_vs_btc"] = beta
        df["resid_vol_vs_btc"] = resid_vol

        # regime shift using returns
        df["regime_shift"] = compute_regime_shift(df["returns"], short=30, long=90)

        # lagged corr delta relative to btc
        # compute using close series if present
        if "close" in df.columns and "close" in btc_aligned.columns:
            df["lagged_corr_delta_vs_btc"] = compute_lagged_corr_delta(btc_aligned["close"], df["close"], short_window=24, long_window=72)
        else:
            df["lagged_corr_delta_vs_btc"] = np.nan

    else:
        # no btc ref available — populate NaNs for BTC-relative features
        df["vol_ratio_vs_btc"] = np.nan
        df["rolling_beta_vs_btc"] = np.nan
        df["resid_vol_vs_btc"] = np.nan
        df["regime_shift"] = np.nan
        df["lagged_corr_delta_vs_btc"] = np.nan

    # cleanup small helper columns? keep all since we want full fidelity
    return df


# -----------------------------
# Memory check helper (decide precompute vs onthefly)
# -----------------------------
def memory_suggest_precompute(estimated_bytes_needed: int) -> bool:
    """
    If psutil available, return True if we think we have enough free memory to hold estimated_bytes_needed.
    Conservative check: require at least 1.5x of estimated_bytes_needed available.
    """
    if not PSUTIL:
        return False
    vm = psutil.virtual_memory()
    avail = vm.available
    return avail > (estimated_bytes_needed * 1.5)


# -----------------------------
# Core streaming builder (full-featured) - preserved and extended
# -----------------------------
def build_master_for_symbol_interval(
    symbol: str,
    interval: str,
    symbols: List[str],
    intervals: List[str],
    out_dir: Path,
    compute_crosscorr: bool = False,
    crosscorr_mode: str = "onthefly",  # "onthefly" or "precompute"
    crosscorr_windows_override: Optional[List[int]] = None,
    row_chunk: int = 50000,
    verbose: bool = False,
    resume: bool = False,
):
    global VERBOSE
    VERBOSE = verbose

    log(f"▶ Building master for {symbol}@{interval} {mem_status()}")
    t0 = time.time()

    # Load anchor target (the main feature file)
    target_df = load_feature_file(symbol, interval)
    if target_df.empty:
        log(f"⚠️ Missing anchor features file for {symbol}_{interval}; skipping.")
        return
    nrows = len(target_df)
    log(f"Target rows: {nrows:,}")

    # Pre-load BTC reference full file for this interval (used for baseline features)
    btc_full = None
    if REFERENCE_SYMBOL != symbol:
        btc_full = load_feature_file(REFERENCE_SYMBOL, interval)
        if btc_full.empty:
            btc_full = None

    # Prepare peers list (paths & meta)
    peers = []
    for src_int in intervals:
        for src_sym in symbols:
            if src_sym == symbol and src_int == interval:
                continue
            p = FEATURES_DIR / f"{src_sym}_{src_int}_features.parquet"
            if p.exists():
                peers.append((src_sym, src_int, p))
    log(f"Peers discovered: {len(peers)}")

    # Decide windows
    if crosscorr_windows_override:
        windows = crosscorr_windows_override
    else:
        windows = CROSSCORR_WINDOWS.get(interval, [120])

    # Setup output paths
    tmp_path = out_dir / f"{symbol}_{interval}_master_tmp.parquet"
    final_path = out_dir / f"{symbol}_{interval}_master_features.parquet"
    schema_ref_path = out_dir / f"{symbol}_schema_columns.txt"
    # safe cleanup of leftover tmp if resume==False
    if tmp_path.exists() and not resume:
        try:
            tmp_path.unlink()
        except Exception:
            pass

    if not PYARROW:
        raise RuntimeError("pyarrow is required for streaming parquet writes (pip install pyarrow)")

    # CROSSCORR: Precompute mode (estimate memory and optionally compute)
    crosscorr_map: Dict[Tuple[str, str], Dict[int, np.ndarray]] = {}
    precomputed_success = False
    if compute_crosscorr and crosscorr_mode == "precompute":
        same_interval_peers = [(ps, pi, pp) for (ps, pi, pp) in peers if pi == interval]
        est_bytes = 0
        for _ in same_interval_peers:
            est_bytes += len(target_df) * 8 * max(1, len(windows))
        log(f"Estimated bytes for precompute: {human_bytes(est_bytes)}")
        if memory_suggest_precompute(est_bytes):
            log("Memory OK → proceeding with precompute crosscorr arrays (NUMBA accelerated if available).")
            target_close = target_df["close"].astype(float).pct_change().fillna(0).to_numpy(dtype=np.float64)
            for (peer_sym, peer_int, peer_path) in same_interval_peers:
                try:
                    p_df = pd.read_parquet(peer_path)
                    ensure_datetime(p_df, "open_time")
                    p_df = p_df.sort_values("open_time").reset_index(drop=True)
                    aligned = align_to_target_times(p_df, target_df["open_time"])
                    if "close" in aligned.columns:
                        close_col = "close"
                    else:
                        close_cols = [c for c in aligned.columns if "close" in c]
                        close_col = close_cols[0] if close_cols else None
                    if close_col is None:
                        log(f"⚠️ peer {peer_sym}-{peer_int} no close col after align → skip precompute")
                        continue
                    peer_close = aligned[close_col].astype(float).pct_change().fillna(0).to_numpy(dtype=np.float64)
                    cw = {}
                    for w in windows:
                        try:
                            arr = rolling_corr(target_close, peer_close, w, min_periods=max(5, int(w * 0.1)))
                            cw[w] = arr
                        except Exception as e:
                            log(f"⚠ precompute rolling_corr failed {peer_sym} w={w}: {e}")
                            cw[w] = np.full(len(target_close), np.nan)
                    crosscorr_map[(peer_sym, peer_int)] = cw
                    del p_df, aligned, peer_close
                    gc.collect()
                except Exception as e:
                    log(f"⚠ precompute failed for {peer_sym}-{peer_int}: {e}")
                    continue
            precomputed_success = True
            log(f"Precomputed crosscorr entries: {len(crosscorr_map)} {mem_status()}")
        else:
            log("Not enough memory for precompute -> switching to onthefly mode.")
            crosscorr_mode = "onthefly"

    # Build chunk function — this is where we apply enhancements to the anchor and all peers
    def build_chunk_df(start: int, end: int) -> pd.DataFrame:
        t_slice = target_df.iloc[start:end].reset_index(drop=True)
        times = t_slice["open_time"]

        # anchor target prefixed
        pref_target = prefix_cols_for_source(t_slice, prefix=interval, symbol_suffix=symbol, exclude=["open_time"])
        chunk = pref_target.copy()
        if "open_time" not in chunk.columns:
            chunk.insert(0, "open_time", times.values)
        else:
            chunk["open_time"] = times.values

        # Prepare BTC aligned for this chunk (baseline)
        btc_aligned = None
        if btc_full is not None:
            try:
                btc_aligned = align_to_target_times(btc_full, times)
                # enrich btc_aligned with its enhancements (so peers can reference returns/atr etc)
                btc_aligned = add_enhancements_to_aligned(btc_aligned, btc_aligned)  # self reference
            except Exception as e:
                log(f"⚠ btc align failed in chunk [{start}:{end}]: {e}")
                btc_aligned = None

        # append each peer aligned to times (drop their open_time), but first compute enhancements
        for (peer_sym, peer_int, peer_path) in peers:
            try:
                p_df = pd.read_parquet(peer_path)
                ensure_datetime(p_df, "open_time")
                p_df = p_df.sort_values("open_time").reset_index(drop=True)
                aligned = align_to_target_times(p_df, times)

                # compute enhancements for this aligned peer (use btc_aligned as reference if available)
                try:
                    aligned_enh = add_enhancements_to_aligned(aligned, btc_aligned)
                except Exception as e:
                    log(f"⚠ enhancement compute failed for {peer_sym}-{peer_int}: {e}")
                    aligned_enh = aligned

                # prefix and drop open_time
                pra = prefix_cols_for_source(aligned_enh, prefix=peer_int, symbol_suffix=peer_sym, exclude=["open_time"])
                if "open_time" in pra.columns:
                    pra = pra.drop(columns=["open_time"])

                # concat horizontally
                chunk = pd.concat([chunk, pra], axis=1)

                del p_df, aligned, aligned_enh, pra
                gc.collect()
            except Exception as e:
                log(f"⚠ chunk align failed for {peer_sym}-{peer_int}: {e}")
                continue

        # Add crosscorr columns depending on mode (preserve previous logic)
        if compute_crosscorr:
            if crosscorr_mode == "precompute" and precomputed_success:
                for (peer_sym, peer_int), cw in crosscorr_map.items():
                    for w, arr in cw.items():
                        col_corr = f"crosscorr__{peer_sym}__{w}"
                        col_delta = f"delta_crosscorr__{peer_sym}__{w}"
                        chunk[col_corr] = arr[start:end]
                        chunk[col_delta] = rolling_diff(arr)[start:end]
            else:
                base_close = t_slice["close"].astype(float).pct_change().fillna(0).to_numpy(dtype=np.float64)
                same_interval_peers = [(ps, pi, pp) for (ps, pi, pp) in peers if pi == interval]
                for (peer_sym, peer_int, peer_path) in same_interval_peers:
                    try:
                        p_df = pd.read_parquet(peer_path)
                        ensure_datetime(p_df, "open_time")
                        p_df = p_df.sort_values("open_time").reset_index(drop=True)
                        aligned = align_to_target_times(p_df, t_slice["open_time"])
                        # find close
                        close_col = None
                        if "close" in aligned.columns:
                            close_col = "close"
                        else:
                            cand = [c for c in aligned.columns if "close" in c]
                            close_col = cand[0] if cand else None
                        if close_col is None:
                            log(f"⚠ onthefly: peer {peer_sym}-{peer_int} has no close col")
                            continue
                        peer_close = aligned[close_col].astype(float).pct_change().fillna(0).to_numpy(dtype=np.float64)
                        for w in windows:
                            c = rolling_corr(base_close, peer_close, w, min_periods=max(5, int(w * 0.1)))
                            col_corr = f"crosscorr__{peer_sym}__{w}"
                            col_delta = f"delta_crosscorr__{peer_sym}__{w}"
                            chunk[col_corr] = c
                            chunk[col_delta] = rolling_diff(c)
                        del p_df, aligned, peer_close
                        gc.collect()
                    except Exception as e:
                        log(f"⚠ onthefly corr fail {peer_sym}-{peer_int}: {e}")
                        continue

        return chunk

    # Build first chunk for schema
    CHUNK = min(row_chunk, nrows)
    first_chunk = build_chunk_df(0, CHUNK)
    # Create pyarrow schema & writer
    table = pa.Table.from_pandas(first_chunk, preserve_index=False)
    schema = table.schema
    writer = pq.ParquetWriter(str(tmp_path), schema=schema, compression="snappy")

    # write first chunk
    writer.write_table(table)
    log(f"✅ Wrote chunk 0 [{0}:{CHUNK}] cols={len(first_chunk.columns)} {mem_status()}")
    del first_chunk, table
    gc.collect()

    # Streaming remaining chunks
    start = CHUNK
    chunk_id = 1
    while start < nrows:
        end = min(start + row_chunk, nrows)
        chunk_df = build_chunk_df(start, end)
        try:
            table = pa.Table.from_pandas(chunk_df, schema=schema, preserve_index=False)
            writer.write_table(table)
        except Exception as e:
            log(f"⚠ Schema write failed for chunk {chunk_id} [{start}:{end}]: {e}. Attempting inferred schema fallback.")
            try:
                table = pa.Table.from_pandas(chunk_df, preserve_index=False)
                writer.write_table(table)
            except Exception as e2:
                writer.close()
                raise RuntimeError(f"Fatal: chunk write failed {chunk_id} [{start}:{end}]: {e2}")
        log(f"✅ Wrote chunk {chunk_id} [{start}:{end}] cols={len(chunk_df.columns)} {mem_status()}")
        start = end
        chunk_id += 1
        del chunk_df, table
        gc.collect()

    # close writer and atomic replace
    writer.close()
    safe_atomic_replace(tmp_path, final_path)
    elapsed = time.time() - t0
    log(f"🎯 Completed master for {symbol}-{interval} -> {final_path} rows={nrows:,} time={elapsed:.1f}s {mem_status()}")

    # -------------------------------
    # Validation + schema reference
    # -------------------------------
    try:
        df_check = pd.read_parquet(final_path, columns=None)
        cols = df_check.columns.tolist()
        # open_time duplicates
        open_time_count = cols.count("open_time")
        if open_time_count != 1:
            log(f"⚠ Validation: {final_path} has {open_time_count} open_time columns")
        # duplicates
        dupes = [c for c in cols if cols.count(c) > 1]
        if dupes:
            log(f"⚠ Duplicate column names detected in {final_path}: {set(dupes)}")
        else:
            log(f"🧩 Schema validated: unique columns OK ({len(cols)} cols)")
        # cross-interval schema reference
        current_cols = set(cols)
        if not schema_ref_path.exists():
            with open(schema_ref_path, "w") as f:
                f.write("\n".join(sorted(current_cols)))
            log(f"📘 Saved reference schema for {symbol} ({len(current_cols)} cols)")
        else:
            with open(schema_ref_path, "r") as f:
                ref_cols = set(line.strip() for line in f if line.strip())
            missing_in_current = sorted(ref_cols - current_cols)
            new_in_current = sorted(current_cols - ref_cols)
            if missing_in_current:
                log(f"⚠ {symbol}-{interval}: Missing {len(missing_in_current)} cols vs reference (examples: {missing_in_current[:5]})")
            if new_in_current:
                log(f"ℹ {symbol}-{interval}: Added {len(new_in_current)} new cols (examples: {new_in_current[:5]})")
            if not missing_in_current and not new_in_current:
                log(f"✅ {symbol}-{interval} columns consistent with schema reference.")
    except Exception as e:
        log(f"⚠ Post-write validation failed for {final_path}: {e}")

    gc.collect()
    return final_path


# -----------------------------
# CLI + Orchestration
# -----------------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["full", "incremental"], default="full")
    p.add_argument("--symbols", nargs="*", default=DEFAULT_SYMBOLS)
    p.add_argument("--intervals", nargs="*", default=DEFAULT_INTERVALS)
    p.add_argument("--out-dir", default=str(OUT_DIR))
    p.add_argument("--compute-crosscorr", action="store_true")
    p.add_argument(
        "--crosscorr-mode",
        choices=["onthefly", "precompute"],
        default="onthefly",
        help="onthefly=compute per-chunk (memory-safe), precompute=compute full arrays first (fast but heavy)"
    )
    p.add_argument("--crosscorr-window", type=int, default=0, help="If >0 override windows with single window value")
    p.add_argument("--row-chunk", type=int, default=50000, help="Rows per streamed chunk")
    p.add_argument("--resume", action="store_true", help="(optional) resume from tmp; not recommended by default")
    p.add_argument("--verbose", action="store_true", help="Verbose logs")
    return p.parse_args()


def main():
    args = parse_args()
    global VERBOSE
    VERBOSE = args.verbose
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not PYARROW:
        print("❗ pyarrow is required: pip install pyarrow", file=sys.stderr)
        sys.exit(1)

    crosscorr_windows_override = None
    if args.crosscorr_window and args.crosscorr_window > 0:
        crosscorr_windows_override = [args.crosscorr_window]

    symbols = args.symbols
    intervals = args.intervals

    log(f"Starting master_feature_merge_v6_12_enhanced_full.py (mode={args.mode})")
    log(f"Numba available: {NUMBA} | pyarrow: {PYARROW} | psutil: {PSUTIL}")

    t_all = time.time()
    summary = []
    for s in symbols:
        for intv in intervals:
            try:
                outp = build_master_for_symbol_interval(
                    s,
                    intv,
                    symbols,
                    intervals,
                    out_dir=out_dir,
                    compute_crosscorr=args.compute_crosscorr,
                    crosscorr_mode=args.crosscorr_mode,
                    crosscorr_windows_override=crosscorr_windows_override,
                    row_chunk=args.row_chunk,
                    verbose=args.verbose,
                    resume=args.resume)
                summary.append((s, intv, str(outp) if outp else "skipped"))
            except Exception as e:
                print(f"❌ Error building {s}-{intv}: {e}", file=sys.stderr)
                # continue with next
    log(f"All done in {time.time() - t_all:.1f}s")
    # print concise summary
    print("=== MASTER BUILD SUMMARY ===")
    for s, intv, path in summary:
        print(f"{s}-{intv} -> {path}")
    print("============================")


if __name__ == "__main__":
    main()
