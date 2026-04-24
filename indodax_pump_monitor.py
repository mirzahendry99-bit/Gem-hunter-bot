"""
╔══════════════════════════════════════════════════════════════════╗
║          INDODAX PUMP SIGNAL MONITOR  v1.0                       ║
║                                                                  ║
║  Target    : Semua pair IDR microcap di Indodax                  ║
║  Stack     : Indodax Public API + pandas + Telegram              ║
║  Timeframe : 15m (sinyal utama) + konfirmasi high 24h            ║
║  Runner    : GitHub Actions (cron — gratis, public repo)         ║
║                                                                  ║
║  Signal Logic (SEMUA harus terpenuhi):                           ║
║  1. Volume Spike  — vol candle terakhir ≥ 5× rata vol 24h        ║
║  2. Price Pump    — harga naik ≥ 3% dalam 1 candle 15m           ║
║  3. Breakout      — close melewati high 24 jam sebelumnya        ║
║  4. RSI Cross     — RSI(14) dari zona ≤35 ke ≥50 (3 candle)      ║
║                                                                  ║
║  ENV VARS (opsional — semua punya default):                      ║
║    VOL_SPIKE_MULT      float   default=5.0                       ║
║    PRICE_PUMP_PCT      float   default=3.0                       ║
║    RSI_OVERSOLD        float   default=35.0                      ║
║    RSI_RECOVERY        float   default=50.0                      ║
║    VOL_IDR_MIN         float   default=5_000_000   (5jt IDR)     ║
║    VOL_IDR_MAX         float   default=2_000_000_000 (2M IDR)    ║
║    SCAN_SLEEP_SEC      float   default=0.30                      ║
║    MAX_SIGNALS_PER_RUN int     default=5                         ║
║    REQUEST_TIMEOUT     int     default=10                        ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import logging
import math
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests

# ══════════════════════════════════════════════════════════════════
#  CONFIG & CONSTANTS
# ══════════════════════════════════════════════════════════════════

# — Credentials (wajib ada di GitHub Secrets)
TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID: str   = os.environ["TELEGRAM_CHAT_ID"]
_TG_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# — Signal Thresholds
VOL_SPIKE_MULT = float(os.environ.get("VOL_SPIKE_MULT", "5.0"))   # vol ≥ Nx rata vol 24h
PRICE_PUMP_PCT = float(os.environ.get("PRICE_PUMP_PCT", "3.0"))   # % naik min dalam 1 candle
RSI_OVERSOLD   = float(os.environ.get("RSI_OVERSOLD",   "35.0"))  # RSI harus pernah ≤ nilai ini
RSI_RECOVERY   = float(os.environ.get("RSI_RECOVERY",   "50.0"))  # RSI sekarang harus ≥ nilai ini

# — Volume Filter IDR (microcap range)
VOL_IDR_MIN = float(os.environ.get("VOL_IDR_MIN", "5_000_000"))       # min 5 juta IDR/24h
VOL_IDR_MAX = float(os.environ.get("VOL_IDR_MAX", "2_000_000_000"))   # max 2 miliar IDR/24h

# — Candle Settings
CANDLE_RESOLUTION   = "15"   # 15 menit
CANDLES_24H         = 96     # 96 × 15m = 24 jam
RSI_WARMUP_CANDLES  = 28     # minimum candle untuk RSI(14) stabil
# Total candle yang di-fetch: 24h baseline + RSI warmup + margin
CANDLE_FETCH_COUNT  = CANDLES_24H + RSI_WARMUP_CANDLES + 6   # = 130

# — Runtime
REQUEST_TIMEOUT_SEC  = int(os.environ.get("REQUEST_TIMEOUT",     "10"))
SCAN_SLEEP_SEC       = float(os.environ.get("SCAN_SLEEP_SEC",    "0.30"))
TG_SEND_SLEEP_SEC    = float(os.environ.get("TG_SEND_SLEEP_SEC", "1.0"))
MAX_SIGNALS_PER_RUN  = int(os.environ.get("MAX_SIGNALS_PER_RUN", "5"))

# — Indodax API
INDODAX_TICKER_ALL = "https://indodax.com/api/ticker_all"
INDODAX_SUMMARIES  = "https://indodax.com/api/summaries"
INDODAX_TV_HISTORY = "https://indodax.com/tradingview/history"   # returns "OK" plain text
# Kandidat endpoint OHLCV alternatif (dicoba oleh probe_candle_api)
_CANDLE_ENDPOINT_CANDIDATES = [
    "https://indodax.com/tradingview/history",
    "https://indodax.com/api/chart/history",
    "https://indodax.com/chart/history",
    "https://indodax.com/tradingview/udf/history",
]
# Endpoint aktif yang ditemukan oleh probe (di-set saat runtime)
_ACTIVE_CANDLE_ENDPOINT: str = ""

# — Timezone
WIB = timezone(timedelta(hours=7))


# ══════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
_logger = logging.getLogger("pump_monitor")


_LOG_LEVELS: dict[str, int] = {
    "debug":   logging.DEBUG,
    "info":    logging.INFO,
    "warn":    logging.WARNING,
    "warning": logging.WARNING,
    "error":   logging.ERROR,
}

def log(msg: str, level: str = "info") -> None:
    """Structured log wrapper. level: debug | info | warn | warning | error."""
    _logger.log(_LOG_LEVELS.get(level, logging.INFO), msg)


# ══════════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════════

def tg(message: str) -> bool:
    """
    Kirim pesan HTML ke Telegram dengan retry 3×.
    Return True jika berhasil terkirim.
    """
    url     = f"{_TG_BASE}/sendMessage"
    payload = {
        "chat_id":                  TELEGRAM_CHAT_ID,
        "text":                     message,
        "parse_mode":               "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(1, 4):
        try:
            resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SEC)
            if resp.status_code == 200:
                return True
            if resp.status_code == 429:
                retry_after = resp.json().get("parameters", {}).get("retry_after", 5)
                log(f"Telegram rate-limited — tunggu {retry_after}s", "warn")
                time.sleep(retry_after)
                continue
            log(f"Telegram HTTP {resp.status_code} (attempt {attempt}): {resp.text[:200]}", "warn")
        except requests.RequestException as e:
            log(f"Telegram error (attempt {attempt}): {e}", "warn")
        time.sleep(2 ** attempt)
    log("Telegram: semua 3 attempt gagal.", "error")
    return False


def validate_telegram() -> bool:
    """
    Validasi token dan chat_id Telegram sebelum scan dimulai.
    Fail-fast dengan pesan diagnosis yang jelas.
    Return True jika valid, False jika ada masalah.
    """
    # 1. Cek token via getMe
    try:
        resp = requests.get(
            f"{_TG_BASE}/getMe",
            timeout=REQUEST_TIMEOUT_SEC,
        )
        if resp.status_code == 404:
            log("❌ TELEGRAM_BOT_TOKEN tidak valid (404). "
                "Buat token baru via @BotFather lalu update GitHub Secret.", "error")
            return False
        if resp.status_code == 401:
            log("❌ TELEGRAM_BOT_TOKEN tidak diotorisasi (401). "
                "Pastikan token lengkap dan tidak ada spasi.", "error")
            return False
        if resp.status_code != 200:
            log(f"❌ Telegram getMe gagal HTTP {resp.status_code}: {resp.text[:100]}", "error")
            return False
        bot_name = resp.json().get("result", {}).get("username", "?")
        log(f"✅ Telegram token valid — bot: @{bot_name}")
    except Exception as e:
        log(f"❌ Telegram getMe exception: {e}", "error")
        return False

    # 2. Cek chat_id dengan kirim pesan tes (sendChatAction — tidak mengirim pesan nyata)
    try:
        resp = requests.post(
            f"{_TG_BASE}/sendChatAction",
            json={"chat_id": TELEGRAM_CHAT_ID, "action": "typing"},
            timeout=REQUEST_TIMEOUT_SEC,
        )
        if resp.status_code == 400:
            log(f"❌ TELEGRAM_CHAT_ID tidak valid: {resp.text[:150]}. "
                "Pastikan bot sudah di-start di chat/channel tersebut.", "error")
            return False
        if resp.status_code != 200:
            log(f"❌ Chat ID check gagal HTTP {resp.status_code}: {resp.text[:100]}", "error")
            return False
        log(f"✅ Telegram chat_id valid — siap kirim notifikasi")
    except Exception as e:
        log(f"❌ Telegram chat check exception: {e}", "error")
        return False

    return True


# ══════════════════════════════════════════════════════════════════
#  INDODAX API
# ══════════════════════════════════════════════════════════════════

def fetch_all_tickers() -> dict[str, dict]:
    """
    Ambil semua ticker dari Indodax.
    Return dict { 'btc_idr': {last, vol_idr, high, low, ...}, ... }
    hanya untuk pair IDR.
    """
    try:
        resp = requests.get(INDODAX_TICKER_ALL, timeout=REQUEST_TIMEOUT_SEC)
        resp.raise_for_status()
        data     = resp.json()
        tickers  = data.get("tickers", {})
        # Filter: hanya pair IDR
        idr_only = {k: v for k, v in tickers.items() if k.endswith("_idr")}
        return idr_only
    except Exception as e:
        log(f"fetch_all_tickers() failed: {e}", "error")
        return {}


def probe_candle_api() -> bool:
    """
    Cari endpoint OHLCV Indodax yang berfungsi dengan mencoba beberapa kandidat URL.
    Jika ditemukan, set _ACTIVE_CANDLE_ENDPOINT secara global.
    Return True jika ada endpoint yang berhasil return candle data.
    """
    global _ACTIVE_CANDLE_ENDPOINT

    now_ts  = int(time.time())
    from_ts = now_ts - (50 * 15 * 60)   # 12.5 jam ke belakang (50 candle 15m)

    log("🔬 Probe endpoint OHLCV Indodax...")

    for url in _CANDLE_ENDPOINT_CANDIDATES:
        for symbol in ["BTCIDR", "btcidr"]:
            params = {
                "symbol":     symbol,
                "resolution": "15",
                "from":       from_ts,
                "to":         now_ts,
            }
            try:
                resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
                body = resp.content.strip()
                snippet = body[:80].decode("utf-8", errors="replace") if body else "(empty)"
                log(f"  {url.split('indodax.com')[1]}?symbol={symbol} "
                    f"→ HTTP {resp.status_code} | {snippet}")

                if resp.status_code != 200 or not body:
                    continue

                try:
                    data = resp.json()
                    if data.get("s") == "ok" and len(data.get("t", [])) > 0:
                        _ACTIVE_CANDLE_ENDPOINT = url
                        log(f"  ✅ ENDPOINT AKTIF: {url} (symbol={symbol}) "
                            f"— {len(data['t'])} candle BTC/IDR diterima")
                        return True
                    elif isinstance(data, dict):
                        log(f"     JSON OK tapi s={data.get('s')!r}, "
                            f"t_len={len(data.get('t', []))}")
                except Exception:
                    pass   # bukan JSON valid

            except Exception as e:
                log(f"  {url} exception: {e}", "warn")

    log("  ❌ Semua endpoint OHLCV gagal — candle tidak tersedia.", "error")
    log("  ℹ️  Bot akan lanjut dengan TICKER-ONLY mode (sinyal disederhanakan).", "warn")
    return False


def fetch_candles(pair: str) -> Optional[pd.DataFrame]:
    """
    Ambil OHLCV candle 15m dari endpoint aktif yang ditemukan probe_candle_api().
    Return None jika endpoint tidak tersedia atau data tidak cukup.
    """
    if not _ACTIVE_CANDLE_ENDPOINT:
        return None   # probe gagal — tidak ada endpoint OHLCV yang bekerja

    symbol  = pair.replace("_", "").upper()
    now_ts  = int(time.time())
    from_ts = now_ts - (CANDLE_FETCH_COUNT * 15 * 60) - 300

    params = {
        "symbol":     symbol,
        "resolution": CANDLE_RESOLUTION,
        "from":       from_ts,
        "to":         now_ts,
    }

    try:
        resp = requests.get(
            _ACTIVE_CANDLE_ENDPOINT,
            params=params,
            timeout=REQUEST_TIMEOUT_SEC,
        )
        resp.raise_for_status()

        # Guard: body kosong → pair tidak punya data candle di endpoint ini
        raw = resp.content.strip()
        if not raw:
            return None

        try:
            data = resp.json()
        except ValueError:
            # Body ada tapi bukan JSON valid (HTML error page, dll)
            return None

        if data.get("s") != "ok":
            # "no_data" atau status lain — pair baru / tidak aktif
            return None

        t_list = data.get("t", [])
        if len(t_list) < RSI_WARMUP_CANDLES + 5:
            return None   # data terlalu sedikit

        df = pd.DataFrame({
            "time":   t_list,
            "open":   [float(x) for x in data["o"]],
            "high":   [float(x) for x in data["h"]],
            "low":    [float(x) for x in data["l"]],
            "close":  [float(x) for x in data["c"]],
            "volume": [float(x) for x in data["v"]],
        })
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        df = df.sort_values("time").reset_index(drop=True)
        return df

    except requests.HTTPError as e:
        log(f"fetch_candles({pair}) HTTP {e.response.status_code}", "debug")
        return None
    except Exception as e:
        log(f"fetch_candles({pair}): {e}", "debug")
        return None


# ══════════════════════════════════════════════════════════════════
#  SIGNAL GATES
# ══════════════════════════════════════════════════════════════════

def gate_volume_spike(df: pd.DataFrame) -> tuple[bool, float]:
    """
    Gate 1 — Volume Spike.
    Vol candle terakhir ≥ VOL_SPIKE_MULT × rata-rata vol 96 candle sebelumnya.

    Return (passed, ratio)
    """
    if len(df) < CANDLES_24H + 1:
        return False, 0.0

    baseline_vols = df["volume"].iloc[-(CANDLES_24H + 1):-1].values
    avg_vol       = baseline_vols.mean()

    if avg_vol <= 0:
        return False, 0.0

    curr_vol = df["volume"].iloc[-1]
    if curr_vol <= 0 or math.isnan(curr_vol):
        return False, 0.0

    ratio = curr_vol / avg_vol
    return ratio >= VOL_SPIKE_MULT, round(ratio, 2)


def gate_price_pump(df: pd.DataFrame) -> tuple[bool, float]:
    """
    Gate 2 — Price Pump.
    Harga close terakhir naik ≥ PRICE_PUMP_PCT% vs candle sebelumnya.

    Return (passed, pct_change)
    """
    if len(df) < 2:
        return False, 0.0

    prev_close = df["close"].iloc[-2]
    curr_close = df["close"].iloc[-1]

    if prev_close <= 0 or math.isnan(prev_close):
        return False, 0.0

    pct = (curr_close - prev_close) / prev_close * 100
    return pct >= PRICE_PUMP_PCT, round(pct, 2)


def gate_breakout(df: pd.DataFrame) -> tuple[bool, float]:
    """
    Gate 3 — Breakout.
    Close terkini melewati high tertinggi 96 candle sebelumnya (24 jam).

    Return (passed, high_24h)
    """
    if len(df) < CANDLES_24H + 1:
        return False, 0.0

    prev_highs = df["high"].iloc[-(CANDLES_24H + 1):-1].values
    high_24h   = float(prev_highs.max())

    if high_24h <= 0:
        return False, 0.0

    curr_close = df["close"].iloc[-1]
    return curr_close > high_24h, round(high_24h, 8)


def _calc_rsi(close: pd.Series, length: int = 14) -> pd.Series:
    """
    Hitung RSI(length) menggunakan metode Wilder's Smoothed Moving Average.
    Pure pandas — tidak butuh library eksternal.
    """
    delta  = close.diff()
    gain   = delta.clip(lower=0)
    loss   = (-delta).clip(lower=0)
    # Wilder smoothing (EMA dengan alpha = 1/length)
    avg_gain = gain.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()
    rs  = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    return rsi


def gate_rsi_cross(df: pd.DataFrame) -> tuple[bool, float]:
    """
    Gate 4 — RSI Cross.
    RSI(14) bergerak dari zona ≤ RSI_OVERSOLD ke ≥ RSI_RECOVERY
    dalam 3 candle terakhir.

    Kondisi:
      - min(RSI[-4:-1]) ≤ RSI_OVERSOLD  (ada yang pernah oversold)
      - RSI[-1] ≥ RSI_RECOVERY           (sekarang sudah recovery)

    Return (passed, current_rsi)
    """
    if len(df) < RSI_WARMUP_CANDLES + 5:
        return False, 0.0

    rsi_series = _calc_rsi(df["close"], length=14)

    rsi_vals = rsi_series.dropna().values
    if len(rsi_vals) < 5:
        return False, 0.0

    curr_rsi   = float(rsi_vals[-1])
    recent_low = float(min(rsi_vals[-5:-1]))   # min RSI dari 4 candle sebelum candle terakhir

    if math.isnan(curr_rsi) or math.isnan(recent_low):
        return False, 0.0

    crossed = (recent_low <= RSI_OVERSOLD) and (curr_rsi >= RSI_RECOVERY)
    return crossed, round(curr_rsi, 1)


# ══════════════════════════════════════════════════════════════════
#  PAIR ANALYZER
# ══════════════════════════════════════════════════════════════════

def _analyze_ticker_only(
    pair: str,
    last: float,
    vol_idr: float,
    high_24h: float,
    low_24h: float,
    ticker_data: dict,
    reject_fn,
) -> Optional[dict]:
    """
    Signal mode fallback ketika candle OHLCV tidak tersedia.
    Menggunakan data ticker 24h saja.

    Kriteria (lebih longgar karena data terbatas):
      1. Price bounce   — last ≥ low_24h × (1 + PRICE_PUMP_PCT/100)
      2. Near breakout  — last ≥ high_24h × 0.97   (dalam 3% dari 24h high)
      3. Vol activity   — vol_idr ≥ VOL_IDR_MIN × 2 (setidaknya 2× floor microcap)
    RSI tidak bisa dihitung tanpa candle — field diisi 0.
    """
    if low_24h <= 0 or high_24h <= 0:
        reject_fn("TICKER_missing_high_low")
        return None

    # Kriteria 1: bounce dari low
    bounce_pct = (last - low_24h) / low_24h * 100
    if bounce_pct < PRICE_PUMP_PCT:
        reject_fn(f"TICKER_bounce_too_small ({bounce_pct:.1f}%<{PRICE_PUMP_PCT}%)")
        return None

    # Kriteria 2: near/at 24h high (breakout territory)
    high_proximity = last / high_24h
    if high_proximity < 0.97:
        reject_fn(f"TICKER_not_near_high ({high_proximity:.2f}<0.97)")
        return None

    # Kriteria 3: volume harus cukup aktif
    if vol_idr < VOL_IDR_MIN * 2:
        reject_fn("TICKER_vol_too_low")
        return None

    coin = pair.replace("_idr", "").upper()
    return {
        "pair":      pair,
        "coin":      coin,
        "price":     last,
        "vol_idr":   vol_idr,
        "vol_ratio": 0.0,    # tidak tersedia tanpa candle
        "pump_pct":  round(bounce_pct, 2),
        "high_24h":  high_24h,
        "rsi":       0.0,    # tidak tersedia tanpa candle
        "mode":      "TICKER_ONLY",
        "ts":        datetime.now(WIB),
    }


def analyze_pair(pair: str, ticker_data: dict, gate_stats: dict) -> Optional[dict]:
    """
    Jalankan semua gate untuk satu pair IDR.
    gate_stats : dict yang di-update in-place untuk diagnostik penolakan.
    Return dict sinyal jika semua 4 gate lolos, else None.

    Sleep (rate-limit guard) hanya dijalankan setelah candle fetch aktual —
    pair yang di-skip oleh vol filter tidak menyebabkan delay.
    """
    def _reject(reason: str) -> None:
        gate_stats[reason] = gate_stats.get(reason, 0) + 1

    try:
        vol_idr    = float(ticker_data.get("vol_idr", 0) or 0)
        last_price = float(ticker_data.get("last",    0) or 0)
        high_24h   = float(ticker_data.get("high",    0) or 0)
        low_24h    = float(ticker_data.get("low",     0) or 0)

        # Pre-filter cepat di level ticker — tidak fetch candle, tidak sleep
        if last_price <= 0:
            _reject("PRE_price_zero")
            return None
        if not (VOL_IDR_MIN <= vol_idr <= VOL_IDR_MAX):
            _reject("PRE_vol_out_of_range")
            return None

        # ── MODE: TICKER-ONLY (ketika candle endpoint tidak tersedia)
        if not _ACTIVE_CANDLE_ENDPOINT:
            return _analyze_ticker_only(
                pair, last_price, vol_idr, high_24h, low_24h, ticker_data, _reject
            )

        # Fetch candle 15m — sleep SETELAH ini untuk rate-limit guard
        df = fetch_candles(pair)
        time.sleep(SCAN_SLEEP_SEC)   # hanya pair yang benar-benar di-fetch

        if df is None or len(df) < CANDLES_24H + RSI_WARMUP_CANDLES:
            _reject("CANDLE_no_data_or_too_short")
            return None

        # ── Gate 1: Volume Spike
        vol_ok, vol_ratio = gate_volume_spike(df)
        if not vol_ok:
            _reject(f"G1_vol_spike_fail (best={vol_ratio:.1f}×, need {VOL_SPIKE_MULT}×)")
            return None

        # ── Gate 2: Price Pump
        pump_ok, pump_pct = gate_price_pump(df)
        if not pump_ok:
            _reject(f"G2_price_pump_fail (best={pump_pct:.1f}%, need {PRICE_PUMP_PCT}%)")
            return None

        # ── Gate 3: Breakout 24h
        break_ok, high_24h = gate_breakout(df)
        if not break_ok:
            _reject("G3_breakout_fail")
            return None

        # ── Gate 4: RSI Cross
        rsi_ok, curr_rsi = gate_rsi_cross(df)
        if not rsi_ok:
            _reject(f"G4_rsi_cross_fail (curr={curr_rsi:.1f})")
            return None

        # Semua gate lolos
        coin = pair.replace("_idr", "").upper()
        return {
            "pair":      pair,
            "coin":      coin,
            "price":     last_price,
            "vol_idr":   vol_idr,
            "vol_ratio": vol_ratio,
            "pump_pct":  pump_pct,
            "high_24h":  high_24h,
            "rsi":       curr_rsi,
            "mode":      "CANDLE",
            "ts":        datetime.now(WIB),
        }

    except Exception as e:
        log(f"analyze_pair({pair}): {e}", "warn")
        _reject("EXCEPTION")
        return None


# ══════════════════════════════════════════════════════════════════
#  FORMAT PESAN TELEGRAM
# ══════════════════════════════════════════════════════════════════

def _fmt_idr(val: float) -> str:
    """Singkat nilai IDR: 1_500_000 → '1.5jt', 2_000_000_000 → '2.0M'."""
    if val >= 1_000_000_000:
        return f"{val / 1_000_000_000:.1f}M IDR"
    if val >= 1_000_000:
        return f"{val / 1_000_000:.1f}jt IDR"
    return f"{val:,.0f} IDR"


def format_signal(sig: dict) -> str:
    """Format satu sinyal pump untuk dikirim ke Telegram (HTML)."""
    ts           = sig["ts"].strftime("%d/%m/%Y %H:%M WIB")
    pair_display = sig["pair"].replace("_", "/").upper()
    mode         = sig.get("mode", "CANDLE")
    mode_label   = "📡 <i>Ticker-only mode</i>" if mode == "TICKER_ONLY" else ""

    rsi_line = (
        f"📊 RSI(14)   : <b>{sig['rsi']:.1f}</b> "
        f"(recovery dari &lt;={RSI_OVERSOLD:.0f})\n"
        if sig["rsi"] > 0 else
        f"📊 RSI(14)   : <i>tidak tersedia (no candle data)</i>\n"
    )
    vol_line = (
        f"🔊 Vol Spike : <b>{sig['vol_ratio']:.1f}×</b> avg vol 24h\n"
        if sig["vol_ratio"] > 0 else
        f"🔊 Vol 24h   : {_fmt_idr(sig['vol_idr'])}\n"
    )

    return (
        f"🚨 <b>PUMP SIGNAL — {sig['coin']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💱 Pair      : <code>{pair_display}</code>\n"
        f"💰 Harga     : <b>{sig['price']:,.2f} IDR</b>\n"
        f"📈 Pump/Bounce: <b>+{sig['pump_pct']:.2f}%</b>\n"
        f"{vol_line}"
        f"🔓 High 24h  : {sig['high_24h']:,.2f} IDR\n"
        f"{rsi_line}"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{mode_label + chr(10) if mode_label else ''}"
        f"⏰ {ts}\n"
        f"⚠️ <i>Bukan saran investasi. DYOR.</i>"
    )


def format_summary(scanned: int, candidates: int, sent: int) -> str:
    """Ringkasan scan yang dikirim setelah semua sinyal."""
    ts = datetime.now(WIB).strftime("%d/%m/%Y %H:%M WIB")
    status = "✅ Ada sinyal!" if sent > 0 else "📭 Tidak ada sinyal"
    return (
        f"📊 <b>SCAN SELESAI</b>  |  {status}\n"
        f"Pair diperiksa : {scanned}\n"
        f"Sinyal ditemukan : {candidates}\n"
        f"Terkirim        : {sent}\n"
        f"⏰ {ts}"
    )


# ══════════════════════════════════════════════════════════════════
#  MAIN RUNNER
# ══════════════════════════════════════════════════════════════════

def run_scan() -> None:
    """Scan semua pair IDR Indodax dan kirim sinyal pump ke Telegram."""
    log("=" * 64)
    log("🔍 INDODAX PUMP SIGNAL MONITOR v1.0 — mulai scan")
    log(
        f"  Threshold: vol≥{VOL_SPIKE_MULT}× | pump≥{PRICE_PUMP_PCT}% | "
        f"RSI {RSI_OVERSOLD:.0f}→{RSI_RECOVERY:.0f}"
    )
    log(
        f"  Vol filter IDR: {_fmt_idr(VOL_IDR_MIN)} – {_fmt_idr(VOL_IDR_MAX)}"
    )
    log("=" * 64)

    # ── Step 0: Validasi Telegram sebelum scan (fail-fast)
    if not validate_telegram():
        log("Scan dibatalkan — perbaiki konfigurasi Telegram terlebih dahulu.", "error")
        sys.exit(1)

    # ── Step 0b: Probe API candle untuk diagnosa
    probe_candle_api()   # log-only, tidak batalkan scan

    # ── Step 1: Fetch semua ticker
    log("Fetching semua ticker Indodax...")
    tickers = fetch_all_tickers()
    if not tickers:
        log("Gagal fetch tickers — abort.", "error")
        tg("❌ <b>Pump Monitor</b>\nGagal fetch ticker dari Indodax.")
        return

    total_idr = len(tickers)
    log(f"Total pair IDR ditemukan: {total_idr}")

    # ── Step 2: Scan setiap pair
    scanned    = 0
    fetched    = 0
    candidates: list[dict] = []
    gate_stats: dict[str, int] = {}

    for pair, ticker_data in tickers.items():
        try:
            vol_idr = float(ticker_data.get("vol_idr", 0) or 0)
            if (VOL_IDR_MIN <= vol_idr <= VOL_IDR_MAX):
                fetched += 1
            scanned += 1
            sig = analyze_pair(pair, ticker_data, gate_stats)
            if sig:
                candidates.append(sig)
                log(
                    f"  ✔ {pair:20s} pump={sig['pump_pct']:+.1f}% "
                    f"vol×{sig['vol_ratio']:.1f} RSI={sig['rsi']:.1f}",
                    "info",
                )

        except Exception as e:
            log(f"  [{pair}] unexpected error: {e}", "warn")

    log(
        f"\nScan selesai: {scanned} pair diperiksa | "
        f"{fetched} candle di-fetch | "
        f"{len(candidates)} sinyal ditemukan"
    )

    # ── Diagnostik: kenapa pair ditolak (selalu tampil)
    if gate_stats:
        log("─" * 56)
        log("📊 GATE REJECTION BREAKDOWN:")
        total_rejected = sum(gate_stats.values())
        for reason, count in sorted(gate_stats.items(), key=lambda x: -x[1]):
            pct = count / scanned * 100 if scanned else 0
            bar = "█" * min(int(pct / 3), 20)
            log(f"  {reason:<45} {count:>4}× ({pct:.0f}%) {bar}")
        log(f"  {'TOTAL DITOLAK':<45} {total_rejected:>4}×")
        log("─" * 56)

    # ── Step 3: Kirim sinyal ke Telegram (max MAX_SIGNALS_PER_RUN)
    sent = 0
    for sig in candidates[:MAX_SIGNALS_PER_RUN]:
        if tg(format_signal(sig)):
            sent += 1
            log(f"  📤 {sig['pair']} [{sig['pump_pct']:+.1f}%] terkirim", "info")
        time.sleep(TG_SEND_SLEEP_SEC)

    # ── Step 4: Kirim ringkasan scan
    tg(format_summary(scanned, len(candidates), sent))
    log(f"\n✅ Done — {sent}/{len(candidates)} sinyal terkirim ke Telegram")


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
        log(f"FATAL: {e}", "error")
        log(traceback.format_exc(), "error")
        try:
            tg(
                f"❌ <b>PUMP MONITOR — FATAL ERROR</b>\n"
                f"<code>{str(e)[:300]}</code>\n"
                f"<i>{datetime.now(WIB).strftime('%d/%m/%Y %H:%M WIB')}</i>"
            )
        except Exception:
            pass
        sys.exit(1)
