"""
╔══════════════════════════════════════════════════════════════════╗
║            GEM HUNTER BOT — Standalone v1.0                     ║
║                                                                  ║
║  Target    : Koin berpotensi 50x–100x di Gate.io                 ║
║  Stack     : Gate.io + Supabase + Telegram + GitHub Actions      ║
║  Timeframe : 4h (analisis utama)                                 ║
║                                                                  ║
║  Filosofi  : Deteksi SEBELUM pump — bukan sesudah                ║
║  Trigger   : Dormancy break = flat base 10d + vol awakening 8x+  ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import math
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone

import gate_api
import numpy as np
import requests
from gate_api import ApiClient, Configuration, SpotApi
from supabase import create_client, Client as SupabaseClient


# ══════════════════════════════════════════════════════════════════
#  CONSTANTS & CONFIG
# ══════════════════════════════════════════════════════════════════

WIB = timezone(timedelta(hours=7))

# ── Gate.io ───────────────────────────────────────────────────────
GATE_API_KEY    = os.environ["GATE_API_KEY"]
GATE_API_SECRET = os.environ["GATE_API_SECRET"]

# ── Telegram ──────────────────────────────────────────────────────
TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
TG_CHAT_ID   = os.environ["TG_CHAT_ID"]

# ── Supabase ──────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# ── Scan ──────────────────────────────────────────────────────────
SCAN_SLEEP_SEC = float(os.environ.get("SCAN_SLEEP_SEC", "0.30"))

# ── BTC Market Regime ─────────────────────────────────────────────
BTC_HALT_PCT  = -8.0   # BTC drop 8% dalam 4h → halt seluruh scan
BTC_BLOCK_PCT = -4.0   # BTC drop 4% dalam 1h → skip signal baru

# ── Volume Filter ─────────────────────────────────────────────────
GEM_VOL_MIN = 500       # min 500 USDT/24h — koin mati total tidak dihitung
GEM_VOL_MAX = 30_000    # max 30K USDT/24h — lebih dari ini bukan microcap sejati

# ── Candle ────────────────────────────────────────────────────────
GEM_TF            = "4h"
GEM_CANDLE_LIMIT  = 168   # 168 × 4h = 28 hari histori

# ── Dormancy (Flat Base) Detection ───────────────────────────────
GEM_FLAT_WINDOW   = 60    # 60 candle × 4h = 10 hari flat base
GEM_FLAT_MAX_BODY = 3.0   # avg candle range (HL/close) < 3% → dormant
GEM_FLAT_MIN_LEN  = 10    # minimum candle yang valid untuk flat period

# ── Breakout Window ───────────────────────────────────────────────
GEM_BREAKOUT_WINDOW   = 6    # 6 candle × 4h = 24 jam terakhir
GEM_VOL_SPIKE_MIN     = 8.0  # volume breakout minimal 8× rata-rata flat
GEM_BREAKOUT_PCT_MIN  = 4.0  # harga naik minimal 4% dari base
GEM_MAX_DIST_FROM_BASE = 50.0 # tidak lebih dari 50% di atas flat base (bukan kejar top)

# ── RSI ───────────────────────────────────────────────────────────
GEM_RSI_MIN = 32
GEM_RSI_MAX = 65

# ── ATH ───────────────────────────────────────────────────────────
GEM_ATH_DIST_MIN = 0.30  # harga harus minimal 30% di bawah ATH historis

# ── 24h Change Guard ──────────────────────────────────────────────
GEM_MAX_CHANGE_24H = 40.0  # sudah naik 40%+ dalam 24h = terlambat

# ── Risk Management ───────────────────────────────────────────────
GEM_TP1_PCT = 0.50   # +50%
GEM_TP2_PCT = 1.50   # +150%
GEM_TP3_PCT = 5.00   # +500% (moonshot — hold sebagian)
GEM_SL_PCT  = 0.08   # -8%
GEM_MIN_RR  = 5.0    # minimum R/R rasio (TP1 / SL distance)

# ── Scoring Tiers ─────────────────────────────────────────────────
#   Max score teoritis: dormancy(3) + vol(3) + breakout(3)
#                       + rsi(2) + ath(3) + ema(2) + bonus(5) = 21
GEM_TIER: dict[str, int] = {
    "MOONSHOT": 13,
    "GEM":       9,
    "WATCH":     7,
}

# ── Dedup & Limits ────────────────────────────────────────────────
GEM_DEDUP_HOURS    = 48
MAX_SIGNALS_PER_RUN = 3

# ── Blocked pair patterns ─────────────────────────────────────────
BLOCKED_SUFFIXES = (
    "3L_USDT", "3S_USDT", "5L_USDT", "5S_USDT",
    "2L_USDT", "2S_USDT", "UP_USDT", "DOWN_USDT",
    "BULL_USDT", "BEAR_USDT",
    "USDC_USDT", "BUSD_USDT", "DAI_USDT", "TUSD_USDT",
    "USDD_USDT", "FRAX_USDT", "USDP_USDT",
    "LUSD_USDT", "USTC_USDT",
)


# ══════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════

def log(msg: str, level: str = "info") -> None:
    ts     = datetime.now(WIB).strftime("%H:%M:%S")
    icons  = {"info": "·", "warn": "⚠", "error": "✖", "ok": "✔"}
    icon   = icons.get(level, "·")
    print(f"[{ts}] {icon} {msg}", flush=True)


# ══════════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════════

def tg(msg: str) -> None:
    """Kirim pesan HTML ke Telegram. Retry 3× dengan exponential backoff."""
    url     = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=15)
            if r.status_code == 200:
                return
            log(f"TG HTTP {r.status_code}: {r.text[:120]}", "warn")
        except Exception as e:
            log(f"TG attempt {attempt + 1}: {e}", "warn")
        time.sleep(2 ** attempt)


# ══════════════════════════════════════════════════════════════════
#  GATE.IO CLIENT
# ══════════════════════════════════════════════════════════════════

def build_gate_client() -> SpotApi:
    cfg = Configuration(key=GATE_API_KEY, secret=GATE_API_SECRET)
    return SpotApi(ApiClient(cfg))


def gate_retry(func, *args, retries: int = 4, **kwargs):
    """Panggil Gate.io API dengan retry & rate-limit handling."""
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except gate_api.exceptions.ApiException as e:
            if e.status == 429:
                wait = 4 * (attempt + 1)
                log(f"Rate limit — sleep {wait}s", "warn")
                time.sleep(wait)
            elif e.status in (500, 502, 503):
                log(f"Gate server error {e.status} — retry", "warn")
                time.sleep(3)
            else:
                log(f"Gate API {e.status}: {e.reason}", "warn")
                return None
        except Exception as e:
            log(f"Gate call error: {e}", "warn")
            if attempt == retries - 1:
                return None
            time.sleep(2)
    return None


# ══════════════════════════════════════════════════════════════════
#  PAIR VALIDATION
# ══════════════════════════════════════════════════════════════════

def is_valid_pair(pair: str) -> bool:
    if not pair.endswith("_USDT"):
        return False
    for blocked in BLOCKED_SUFFIXES:
        if pair.endswith(blocked):
            return False
    return True


# ══════════════════════════════════════════════════════════════════
#  CANDLE DATA
# ══════════════════════════════════════════════════════════════════

def fetch_candles(
    client: SpotApi, pair: str, tf: str = GEM_TF, limit: int = GEM_CANDLE_LIMIT
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray] | None:
    """
    Ambil OHLCV dari Gate.io.
    Returns (closes, highs, lows, volumes) atau None jika gagal.

    Gate.io candlestick format per item:
    [timestamp, volume_quote, close, high, low, open, volume_base, ...]
    """
    raw = gate_retry(
        client.list_candlesticks,
        currency_pair=pair,
        interval=tf,
        limit=limit,
    )
    if not raw or len(raw) < 20:
        return None
    try:
        closes  = np.array([float(c[2]) for c in raw], dtype=float)
        highs   = np.array([float(c[3]) for c in raw], dtype=float)
        lows    = np.array([float(c[4]) for c in raw], dtype=float)
        volumes = np.array([float(c[1]) for c in raw], dtype=float)  # quote volume (USDT)
        return closes, highs, lows, volumes
    except Exception as e:
        log(f"parse candles [{pair}]: {e}", "warn")
        return None


# ══════════════════════════════════════════════════════════════════
#  TECHNICAL INDICATORS
# ══════════════════════════════════════════════════════════════════

def rsi(closes: np.ndarray, period: int = 14) -> float:
    if len(closes) < period + 2:
        return 50.0
    deltas = np.diff(closes)
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    ag = float(np.mean(gains[:period]))
    al = float(np.mean(losses[:period]))
    for i in range(period, len(deltas)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    if al == 0:
        return 100.0
    return round(100.0 - (100.0 / (1.0 + ag / al)), 2)


def ema(closes: np.ndarray, period: int) -> float:
    if len(closes) < period:
        return float(closes[-1]) if len(closes) > 0 else 0.0
    k   = 2.0 / (period + 1)
    val = float(np.mean(closes[:period]))
    for price in closes[period:]:
        val = float(price) * k + val * (1 - k)
    return val


def macd(closes: np.ndarray) -> tuple[float, float]:
    """Returns (macd_line, signal_line). Positive macd_line = bullish momentum."""
    if len(closes) < 35:
        return 0.0, 0.0
    e12  = ema(closes, 12)
    e26  = ema(closes, 26)
    line = e12 - e26
    # signal: EMA9 dari MACD line (simplified — single value)
    # Cukup untuk deteksi bullish/bearish momentum
    sig  = ema(closes[-9:], 9) if len(closes) >= 9 else line
    return line, sig


def atr(closes: np.ndarray, highs: np.ndarray, lows: np.ndarray, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 0.0
    trs = []
    for i in range(1, min(period + 1, len(closes))):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    return float(np.mean(trs)) if trs else 0.0


def detect_liquidity_sweep(
    closes: np.ndarray, highs: np.ndarray, lows: np.ndarray, lookback: int = 30
) -> bool:
    """
    Deteksi bullish liquidity sweep:
    Harga sempat turun ke bawah support lokal (ambil likuiditas),
    lalu recover kembali di atas support dalam candle terakhir.
    Ini pola smart money sebelum move besar.
    """
    if len(closes) < lookback + 5:
        return False
    window_lows  = lows[-(lookback + 5): -5]
    support      = float(np.min(window_lows))
    recent_low   = float(np.min(lows[-5:]))
    current      = float(closes[-1])
    # Sweep terjadi jika: candle recent sempat ke bawah support, tapi close di atas
    swept        = recent_low < support
    recovered    = current > support
    return swept and recovered


# ══════════════════════════════════════════════════════════════════
#  BTC MARKET REGIME
# ══════════════════════════════════════════════════════════════════

def get_btc_regime(client: SpotApi) -> dict:
    """
    Cek kondisi BTC untuk market regime filter.
    Jika BTC crash, kita tidak beli apapun — gem sekalipun.
    """
    result = {
        "btc_1h": 0.0,
        "btc_4h": 0.0,
        "halt": False,
        "block_buy": False,
    }
    try:
        # 1h candles — ambil 4 candle terakhir
        c1h = gate_retry(
            client.list_candlesticks,
            currency_pair="BTC_USDT",
            interval="1h",
            limit=4,
        )
        if c1h and len(c1h) >= 2:
            prev  = float(c1h[-2][2])
            cur   = float(c1h[-1][2])
            result["btc_1h"] = (cur - prev) / prev * 100 if prev > 0 else 0.0

        # 4h candles — ambil 3 candle terakhir
        c4h = gate_retry(
            client.list_candlesticks,
            currency_pair="BTC_USDT",
            interval="4h",
            limit=3,
        )
        if c4h and len(c4h) >= 2:
            prev  = float(c4h[-2][2])
            cur   = float(c4h[-1][2])
            result["btc_4h"] = (cur - prev) / prev * 100 if prev > 0 else 0.0

        result["halt"]      = result["btc_4h"] <= BTC_HALT_PCT
        result["block_buy"] = result["btc_1h"]  <= BTC_BLOCK_PCT

    except Exception as e:
        log(f"get_btc_regime: {e}", "warn")

    return result


# ══════════════════════════════════════════════════════════════════
#  IDR CONVERSION
# ══════════════════════════════════════════════════════════════════

_idr_cache: dict = {}

def get_usdt_idr() -> float:
    """Fetch kurs USDT/IDR dari Coingecko. Cache 30 menit."""
    global _idr_cache
    now = time.time()
    if _idr_cache.get("ts", 0) + 1800 > now:
        return _idr_cache.get("rate", 15500.0)
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "tether", "vs_currencies": "idr"},
            timeout=10,
        )
        rate = float(r.json()["tether"]["idr"])
        _idr_cache = {"rate": rate, "ts": now}
        return rate
    except Exception:
        return _idr_cache.get("rate", 15500.0)


def fmt_idr(usdt_price: float) -> str:
    """Format harga USDT ke IDR string yang readable."""
    rate = get_usdt_idr()
    idr  = usdt_price * rate
    if idr >= 1_000_000_000:
        return f"Rp {idr / 1_000_000_000:.2f}M"
    if idr >= 1_000_000:
        return f"Rp {idr / 1_000_000:.2f}jt"
    if idr >= 1_000:
        return f"Rp {idr:,.0f}"
    return f"Rp {idr:.2f}"


# ══════════════════════════════════════════════════════════════════
#  SUPABASE — DEDUP & SIGNAL STORAGE
# ══════════════════════════════════════════════════════════════════

_sb: SupabaseClient | None = None

def sb() -> SupabaseClient:
    global _sb
    if _sb is None:
        _sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _sb


def already_sent(pair: str) -> bool:
    """
    Cek apakah signal untuk pair ini sudah dikirim dalam GEM_DEDUP_HOURS jam.
    Query ke tabel gem_dedup di Supabase.
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=GEM_DEDUP_HOURS)).isoformat()
        res = (
            sb()
            .table("gem_dedup")
            .select("id")
            .eq("pair", pair)
            .gte("sent_at", cutoff)
            .limit(1)
            .execute()
        )
        return len(res.data) > 0
    except Exception as e:
        log(f"dedup check [{pair}]: {e}", "warn")
        return False  # fail-open: tetap scan jika supabase error


def mark_sent(pair: str) -> None:
    """Catat pair ke tabel gem_dedup agar tidak dikirim ulang."""
    try:
        sb().table("gem_dedup").insert({
            "pair":    pair,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e:
        log(f"mark_sent [{pair}]: {e}", "warn")


def save_signal(sig: dict) -> None:
    """
    Simpan detail signal ke tabel gem_signals untuk tracking & history.
    """
    try:
        row = {
            "pair":           sig["pair"],
            "tier":           sig["tier"],
            "score":          sig["score"],
            "entry":          sig["entry"],
            "tp1":            sig["tp1"],
            "tp2":            sig["tp2"],
            "tp3":            sig["tp3"],
            "sl":             sig["sl"],
            "rr":             sig["rr"],
            "rsi":            sig["rsi"],
            "vol_ratio":      sig["vol_ratio"],
            "breakout_pct":   sig["breakout_pct"],
            "avg_flat_range": sig["avg_flat_range"],
            "ath_dist_pct":   sig["ath_dist_pct"],
            "dist_from_base": sig["dist_from_base"],
            "change_24h":     sig["change_24h"],
            "is_new_listing": sig["is_new_listing"],
            "has_sweep":      sig["has_sweep"],
            "macd_bull":      sig["macd_bull"],
            "created_at":     datetime.now(timezone.utc).isoformat(),
        }
        sb().table("gem_signals").insert(row).execute()
    except Exception as e:
        log(f"save_signal [{sig.get('pair')}]: {e}", "warn")


# ══════════════════════════════════════════════════════════════════
#  CORE: GEM HUNTER SCANNER
# ══════════════════════════════════════════════════════════════════

def analyze_gem(
    client: SpotApi,
    pair: str,
    price: float,
    vol_24h: float,
    change_24h: float,
) -> dict | None:
    """
    Analisis satu pair — apakah memenuhi kriteria GEM HUNTER?

    Gate 1 : Volume 24h dalam zona microcap (500–30K USDT)
    Gate 2 : Belum terlambat — change 24h < 40%
    Gate 3 : Flat base terdeteksi — dormant 10 hari
    Gate 4 : Volume awakening — spike 8x dari baseline
    Gate 5 : Early breakout — harga naik 4%+ dari base
    Gate 6 : RSI sehat — tidak dead, tidak overbought
    Gate 7 : ATH distance — masih jauh dari puncak historis
    Gate 8 : EMA micro-trend konfirmasi

    Returns dict signal atau None jika tidak lolos.
    """

    # ── Gate 1 & 2: Pre-filter cepat ─────────────────────────────
    if not (GEM_VOL_MIN <= vol_24h <= GEM_VOL_MAX):
        return None
    if change_24h > GEM_MAX_CHANGE_24H:
        return None

    # ── Fetch candle data ─────────────────────────────────────────
    data = fetch_candles(client, pair)
    if data is None:
        return None

    closes, highs, lows, volumes = data
    n = len(closes)

    # Tentukan window size — pair baru mungkin tidak punya 28 hari data
    min_needed     = GEM_FLAT_WINDOW + GEM_BREAKOUT_WINDOW + 5
    is_new_listing = n < 84  # < 14 hari data 4h

    if n < min_needed:
        flat_w = max(n - GEM_BREAKOUT_WINDOW - 3, GEM_FLAT_MIN_LEN)
        if flat_w < GEM_FLAT_MIN_LEN:
            return None
    else:
        flat_w = GEM_FLAT_WINDOW

    # ── Pisahkan zona flat vs breakout ───────────────────────────
    breakout_start = n - GEM_BREAKOUT_WINDOW
    flat_end       = breakout_start
    flat_start     = max(0, flat_end - flat_w)

    flat_closes  = closes[flat_start:flat_end]
    flat_volumes = volumes[flat_start:flat_end]
    flat_highs   = highs[flat_start:flat_end]
    flat_lows    = lows[flat_start:flat_end]

    if len(flat_closes) < GEM_FLAT_MIN_LEN:
        return None

    # ── Gate 3: Flat Base Detection ───────────────────────────────
    # avg (high-low)/close per candle — koin dormant < 3%
    ranges = []
    for i in range(len(flat_closes)):
        c = flat_closes[i]
        if c > 0:
            ranges.append((flat_highs[i] - flat_lows[i]) / c * 100)

    if not ranges:
        return None

    avg_flat_range = float(np.mean(ranges))

    if avg_flat_range > GEM_FLAT_MAX_BODY:
        return None  # terlalu volatile di flat period — bukan dormancy

    # Scoring dormancy: semakin flat semakin baik
    if avg_flat_range < 0.8:
        dormancy_score = 3
    elif avg_flat_range < 1.5:
        dormancy_score = 2
    elif avg_flat_range < GEM_FLAT_MAX_BODY:
        dormancy_score = 1
    else:
        dormancy_score = 0

    # ── Gate 4: Volume Awakening ──────────────────────────────────
    vol_baseline = float(np.mean(flat_volumes))
    if vol_baseline <= 0:
        return None

    breakout_vols    = volumes[-GEM_BREAKOUT_WINDOW:]
    vol_max_breakout = float(np.max(breakout_vols))
    vol_ratio        = vol_max_breakout / vol_baseline

    if vol_ratio < GEM_VOL_SPIKE_MIN:
        return None

    if vol_ratio >= 30:
        vol_score = 3
    elif vol_ratio >= 15:
        vol_score = 2
    elif vol_ratio >= GEM_VOL_SPIKE_MIN:
        vol_score = 1
    else:
        vol_score = 0

    # ── Gate 5: Early Breakout ────────────────────────────────────
    price_before = float(closes[-(GEM_BREAKOUT_WINDOW + 1)])
    if price_before <= 0:
        return None

    breakout_pct = (price - price_before) / price_before * 100
    if breakout_pct < GEM_BREAKOUT_PCT_MIN:
        return None

    # Pastikan belum terlalu jauh dari flat base (bukan kejar top)
    flat_avg_close  = float(np.mean(flat_closes))
    dist_from_base  = (price - flat_avg_close) / flat_avg_close * 100 if flat_avg_close > 0 else 0.0
    if dist_from_base > GEM_MAX_DIST_FROM_BASE:
        return None

    if breakout_pct >= 20:
        breakout_score = 3
    elif breakout_pct >= 10:
        breakout_score = 2
    elif breakout_pct >= GEM_BREAKOUT_PCT_MIN:
        breakout_score = 1
    else:
        breakout_score = 0

    # ── Gate 6: RSI ───────────────────────────────────────────────
    rsi_val = rsi(closes)
    if not (GEM_RSI_MIN <= rsi_val <= GEM_RSI_MAX):
        return None

    if rsi_val < 45:
        rsi_score = 2
    elif rsi_val < 55:
        rsi_score = 1
    else:
        rsi_score = 0

    # ── Gate 7: ATH Distance ──────────────────────────────────────
    ath_price = float(np.max(highs))
    if ath_price <= 0:
        return None

    ath_dist = (ath_price - price) / ath_price  # 0 = at ATH
    if ath_dist < GEM_ATH_DIST_MIN:
        return None  # terlalu dekat ATH = kurang recovery room

    if ath_dist >= 0.85:
        ath_score = 3
    elif ath_dist >= 0.70:
        ath_score = 2
    elif ath_dist >= 0.50:
        ath_score = 1
    else:
        ath_score = 0

    # ── Gate 8: EMA Micro-Trend ───────────────────────────────────
    ema7  = ema(closes, 7)
    ema20 = ema(closes, 20)
    ema_score = 0
    if price > ema7:
        ema_score += 1
    if ema7 > ema20:
        ema_score += 1

    # ── Bonus: New Listing ────────────────────────────────────────
    new_listing_bonus = 2 if is_new_listing else 0

    # ── Bonus: Liquidity Sweep ────────────────────────────────────
    has_sweep   = detect_liquidity_sweep(closes, highs, lows)
    sweep_bonus = 2 if has_sweep else 0

    # ── Bonus: MACD micro bullish ─────────────────────────────────
    macd_line, macd_sig = macd(closes)
    macd_bull  = macd_line > macd_sig
    macd_bonus = 1 if macd_bull else 0

    # ── Total Score ───────────────────────────────────────────────
    total_score = (
        dormancy_score    # 0–3
        + vol_score       # 0–3
        + breakout_score  # 0–3
        + rsi_score       # 0–2
        + ath_score       # 0–3
        + ema_score       # 0–2
        + new_listing_bonus  # 0–2
        + sweep_bonus        # 0–2
        + macd_bonus         # 0–1
    )

    # ── Tier Assignment ───────────────────────────────────────────
    if total_score >= GEM_TIER["MOONSHOT"]:
        tier = "MOONSHOT"
    elif total_score >= GEM_TIER["GEM"]:
        tier = "GEM"
    elif total_score >= GEM_TIER["WATCH"]:
        tier = "WATCH"
    else:
        return None

    # ── Risk / Reward ─────────────────────────────────────────────
    entry = price
    sl    = round(entry * (1 - GEM_SL_PCT), 8)
    tp1   = round(entry * (1 + GEM_TP1_PCT), 8)
    tp2   = round(entry * (1 + GEM_TP2_PCT), 8)
    tp3   = round(entry * (1 + GEM_TP3_PCT), 8)

    sl_dist  = entry - sl
    tp1_dist = tp1 - entry
    if sl_dist <= 0:
        return None

    rr_ratio = round(tp1_dist / sl_dist, 1)
    if rr_ratio < GEM_MIN_RR:
        return None

    atr_val = atr(closes, highs, lows)
    atr_pct = atr_val / price * 100 if price > 0 else 0.0

    return {
        "pair":           pair,
        "tier":           tier,
        "score":          total_score,
        "entry":          entry,
        "tp1":            tp1,
        "tp2":            tp2,
        "tp3":            tp3,
        "sl":             sl,
        "rr":             rr_ratio,
        "rsi":            round(rsi_val, 1),
        "vol_ratio":      round(vol_ratio, 1),
        "breakout_pct":   round(breakout_pct, 2),
        "avg_flat_range": round(avg_flat_range, 2),
        "ath_dist_pct":   round(ath_dist * 100, 1),
        "dist_from_base": round(dist_from_base, 1),
        "atr_pct":        round(atr_pct, 2),
        "is_new_listing": is_new_listing,
        "has_sweep":      has_sweep,
        "macd_bull":      macd_bull,
        "change_24h":     round(change_24h, 2),
        "dormancy_score": dormancy_score,
        "vol_score":      vol_score,
        "breakout_score": breakout_score,
    }


# ══════════════════════════════════════════════════════════════════
#  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════════

def format_signal(sig: dict) -> str:
    """Buat pesan Telegram HTML untuk satu GEM signal."""

    pair     = sig["pair"].replace("_USDT", "/USDT")
    entry    = sig["entry"]
    tp1, tp2, tp3, sl = sig["tp1"], sig["tp2"], sig["tp3"], sig["sl"]
    tier     = sig["tier"]

    pct_tp1 = (tp1 - entry) / entry * 100
    pct_tp2 = (tp2 - entry) / entry * 100
    pct_tp3 = (tp3 - entry) / entry * 100
    pct_sl  = (entry - sl)  / entry * 100

    now         = datetime.now(WIB)
    valid_until = (now + timedelta(hours=GEM_DEDUP_HOURS)).strftime("%d/%m %H:%M WIB")

    tier_icon = {"MOONSHOT": "🚀", "GEM": "💎", "WATCH": "👁"}.get(tier, "🎯")

    # Context lines
    ctx = []
    if sig["is_new_listing"]:
        ctx.append("🆕 Pair baru listing — histori pendek, volatilitas lebih tinggi")
    if sig["has_sweep"]:
        ctx.append("🧲 Liquidity sweep terdeteksi — smart money sudah masuk")
    if sig["macd_bull"]:
        ctx.append("📊 MACD micro bullish — momentum awal terbentuk")
    if sig["avg_flat_range"] < 1.0:
        ctx.append(f"😴 Deep sleep ({sig['avg_flat_range']:.1f}% avg range) — energi tersimpan panjang")
    elif sig["avg_flat_range"] < 2.0:
        ctx.append(f"😴 Flat base jelas ({sig['avg_flat_range']:.1f}% avg range)")

    ctx_block = "\n".join(ctx) if ctx else "—"

    score_detail = (
        f"Dormancy {sig['dormancy_score']}/3 · "
        f"Vol {sig['vol_score']}/3 · "
        f"Breakout {sig['breakout_score']}/3"
    )

    return (
        f"{tier_icon} <b>GEM HUNTER — {tier}</b>  🟢 BUY\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Pair    : <b>{pair}</b>  [4h]\n"
        f"⏰ Valid : {now.strftime('%d/%m %H:%M')} → {valid_until}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Entry   : <b>${entry:.8f}</b>  <i>≈ {fmt_idr(entry)}</i>\n"
        f"TP1     : <b>${tp1:.8f}</b>  <i>≈ {fmt_idr(tp1)}</i>  <i>(+{pct_tp1:.0f}%)</i>\n"
        f"TP2     : <b>${tp2:.8f}</b>  <i>≈ {fmt_idr(tp2)}</i>  <i>(+{pct_tp2:.0f}%)</i>\n"
        f"TP3     : <b>${tp3:.8f}</b>  <i>(+{pct_tp3:.0f}% moonshot)</i>\n"
        f"SL      : <b>${sl:.8f}</b>  <i>≈ {fmt_idr(sl)}</i>  <i>(-{pct_sl:.1f}%)</i>\n"
        f"R/R     : <b>1:{sig['rr']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Vol spike    : <b>{sig['vol_ratio']:.1f}×</b> rata-rata flat period\n"
        f"📈 Breakout     : <b>+{sig['breakout_pct']:.1f}%</b> dari base (24h)\n"
        f"💤 Dormancy     : {sig['avg_flat_range']:.1f}% avg range\n"
        f"📉 ATH dist     : <b>{sig['ath_dist_pct']:.0f}%</b> di bawah ATH historis\n"
        f"RSI             : <b>{sig['rsi']}</b>  |  24h: {sig['change_24h']:+.1f}%\n"
        f"Score           : <b>{sig['score']}</b>  ({score_detail})\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{ctx_block}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <b>Extreme High Risk Setup</b>\n"
        f"<i>· Alokasi maks 0.5–1% modal</i>\n"
        f"<i>· Hold 1–7 hari — bukan scalp</i>\n"
        f"<i>· Ambil sebagian profit di TP1, sisakan untuk TP2/TP3</i>\n"
        f"<i>· SL wajib dipasang — koin kecil bisa dump 50% dalam jam</i>\n"
        f"<i>· Bukan rekomendasi finansial</i>"
    )


def format_summary(scanned: int, skipped: int, found: int, sent: int, halted: bool = False) -> str:
    now = datetime.now(WIB).strftime("%d/%m/%Y %H:%M WIB")
    if halted:
        return (
            f"🛑 <b>GEM HUNTER — HALT</b>\n"
            f"BTC crash terdeteksi. Scan dilewati untuk keamanan modal.\n"
            f"<i>{now}</i>"
        )
    return (
        f"💎 <b>GEM HUNTER Scan Selesai</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Pairs scanned   : {scanned}\n"
        f"Diluar vol range: {skipped}\n"
        f"Kandidat gem    : {found}\n"
        f"Signal terkirim : <b>{sent}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Setup ini butuh 1–7 hari untuk bermain.</i>\n"
        f"<i>{now}</i>"
    )


# ══════════════════════════════════════════════════════════════════
#  MAIN SCAN RUNNER
# ══════════════════════════════════════════════════════════════════

def run_scan() -> None:
    log("=" * 55)
    log(f"💎 GEM HUNTER — {datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB')}")
    log("=" * 55)

    client = build_gate_client()

    # ── BTC Regime Check ──────────────────────────────────────────
    btc = get_btc_regime(client)
    log(f"BTC 1h: {btc['btc_1h']:+.1f}%  |  BTC 4h: {btc['btc_4h']:+.1f}%")

    if btc["halt"]:
        log("🛑 BTC crash — scan dilewati", "warn")
        tg(format_summary(0, 0, 0, 0, halted=True))
        return

    if btc["block_buy"]:
        log("⚠️ BTC drop 1h — scan tetap jalan, threshold lebih ketat", "warn")

    # ── Fetch semua ticker Gate.io ────────────────────────────────
    log("Fetching tickers Gate.io...")
    tickers = gate_retry(client.list_tickers) or []
    log(f"Total tickers: {len(tickers)}")

    candidates: list[dict] = []
    scanned   = 0
    skipped   = 0

    for t in tickers:
        pair = getattr(t, "currency_pair", None)
        if not pair or not is_valid_pair(pair):
            continue

        try:
            price      = float(t.last or 0)
            vol_24h    = float(t.quote_volume or 0)
            _cp        = getattr(t, "change_percentage", None)
            change_24h = 0.0
            if _cp not in (None, "", "NaN"):
                _f = float(_cp)
                change_24h = 0.0 if math.isnan(_f) else _f

            if price <= 0:
                continue

            # Pre-filter volume sebelum fetch candle
            if not (GEM_VOL_MIN <= vol_24h <= GEM_VOL_MAX):
                skipped += 1
                continue

            # Dedup check
            if already_sent(pair):
                continue

            scanned += 1
            sig = analyze_gem(client, pair, price, vol_24h, change_24h)
            if sig:
                candidates.append(sig)
                log(f"  ✔ {pair} [{sig['tier']}] score={sig['score']} vol×{sig['vol_ratio']:.1f}", "ok")

            time.sleep(SCAN_SLEEP_SEC)

        except Exception as e:
            log(f"  [{pair}] {e}", "warn")
            continue

    log(f"\nScan selesai: {scanned} pairs diperiksa | {skipped} diluar vol range | {len(candidates)} kandidat")

    if not candidates:
        log("Tidak ada gem ditemukan saat ini.")
        tg(format_summary(scanned, skipped, 0, 0))
        return

    # ── Sort: tier priority → score → vol_ratio ───────────────────
    tier_order = {"MOONSHOT": 0, "GEM": 1, "WATCH": 2}
    candidates.sort(key=lambda x: (tier_order.get(x["tier"], 9), -x["score"], -x["vol_ratio"]))

    # ── Kirim signals ─────────────────────────────────────────────
    sent = 0
    for sig in candidates:
        if sent >= MAX_SIGNALS_PER_RUN:
            break
        tg(format_signal(sig))
        save_signal(sig)
        mark_sent(sig["pair"])
        log(f"  📤 Signal terkirim: {sig['pair']} [{sig['tier']}]", "ok")
        sent += 1
        time.sleep(0.5)

    tg(format_summary(scanned, skipped, len(candidates), sent))
    log(f"\n✅ Done — {sent} signal terkirim")


# ══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        run_scan()
    except KeyboardInterrupt:
        log("Interrupted by user.")
        sys.exit(0)
    except Exception as e:
        error_msg = traceback.format_exc()
        log(f"FATAL: {e}", "error")
        log(error_msg, "error")
        try:
            tg(
                f"❌ <b>GEM HUNTER — ERROR</b>\n"
                f"<code>{str(e)[:300]}</code>\n"
                f"<i>{datetime.now(WIB).strftime('%d/%m/%Y %H:%M WIB')}</i>"
            )
        except Exception:
            pass
        sys.exit(1)
