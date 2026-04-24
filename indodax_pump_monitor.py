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
INDODAX_TV_HISTORY = "https://indodax.com/tradingview/history"

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
                # Rate limited — tunggu sesuai retry_after
                retry_after = resp.json().get("parameters", {}).get("retry_after", 5)
                log(f"Telegram rate-limited — tunggu {retry_after}s", "warn")
                time.sleep(retry_after)
                continue
            log(f"Telegram HTTP {resp.status_code} (attempt {attempt}): {resp.text[:200]}", "warn")
        except requests.RequestException as e:
            log(f"Telegram error (attempt {attempt}): {e}", "warn")
        time.sleep(2 ** attempt)   # backoff: 2s, 4s, 8s
    log("Telegram: semua 3 attempt gagal.", "error")
    return False


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


def fetch_candles(pair: str) -> Optional[pd.DataFrame]:
    """
    Ambil OHLCV candle 15m dari Indodax TradingView API.

    pair   : format 'btc_idr'  → symbol TradingView 'BTCIDR'
    Return : DataFrame kolom [time, open, high, low, close, volume]
             di-sort ascending by time.
             None jika data tidak cukup atau request gagal.
    """
    symbol  = pair.replace("_", "").upper()   # btc_idr → BTCIDR
    now_ts  = int(time.time())
    from_ts = now_ts - (CANDLE_FETCH_COUNT * 15 * 60) - 300   # buffer 5 menit

    params = {
        "symbol":     symbol,
        "resolution": CANDLE_RESOLUTION,
        "from":       from_ts,
        "to":         now_ts,
        "countback":  CANDLE_FETCH_COUNT,
    }
    try:
        resp = requests.get(
            INDODAX_TV_HISTORY,
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

def analyze_pair(pair: str, ticker_data: dict) -> Optional[dict]:
    """
    Jalankan semua gate untuk satu pair IDR.
    Return dict sinyal jika semua 4 gate lolos, else None.

    Sleep (rate-limit guard) hanya dijalankan setelah candle fetch aktual —
    pair yang di-skip oleh vol filter tidak menyebabkan delay.
    """
    try:
        vol_idr    = float(ticker_data.get("vol_idr", 0) or 0)
        last_price = float(ticker_data.get("last",    0) or 0)

        # Pre-filter cepat di level ticker — tidak fetch candle, tidak sleep
        if last_price <= 0:
            return None
        if not (VOL_IDR_MIN <= vol_idr <= VOL_IDR_MAX):
            return None

        # Fetch candle 15m — sleep SETELAH ini untuk rate-limit guard
        df = fetch_candles(pair)
        time.sleep(SCAN_SLEEP_SEC)   # hanya pair yang benar-benar di-fetch

        if df is None or len(df) < CANDLES_24H + RSI_WARMUP_CANDLES:
            return None

        # ── Gate 1: Volume Spike
        vol_ok, vol_ratio = gate_volume_spike(df)
        if not vol_ok:
            return None

        # ── Gate 2: Price Pump
        pump_ok, pump_pct = gate_price_pump(df)
        if not pump_ok:
            return None

        # ── Gate 3: Breakout 24h
        break_ok, high_24h = gate_breakout(df)
        if not break_ok:
            return None

        # ── Gate 4: RSI Cross
        rsi_ok, curr_rsi = gate_rsi_cross(df)
        if not rsi_ok:
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
            "ts":        datetime.now(WIB),
        }

    except Exception as e:
        log(f"analyze_pair({pair}): {e}", "warn")
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

    return (
        f"🚨 <b>PUMP SIGNAL — {sig['coin']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💱 Pair      : <code>{pair_display}</code>\n"
        f"💰 Harga     : <b>{sig['price']:,.2f} IDR</b>\n"
        f"📈 Pump 15m  : <b>+{sig['pump_pct']:.2f}%</b>\n"
        f"🔊 Vol Spike : <b>{sig['vol_ratio']:.1f}×</b> avg vol 24h\n"
        f"🔓 Breakout  : close &gt; high 24h ({sig['high_24h']:,.2f})\n"
        f"📊 RSI(14)   : <b>{sig['rsi']:.1f}</b> "
        f"(recovery dari &lt;={RSI_OVERSOLD:.0f})\n"
        f"💵 Vol 24h   : {_fmt_idr(sig['vol_idr'])}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
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
    fetched    = 0   # hanya pair yang benar-benar di-fetch candle-nya
    candidates: list[dict] = []

    for pair, ticker_data in tickers.items():
        try:
            vol_idr = float(ticker_data.get("vol_idr", 0) or 0)
            if (VOL_IDR_MIN <= vol_idr <= VOL_IDR_MAX):
                fetched += 1
            scanned += 1
            sig = analyze_pair(pair, ticker_data)
            if sig:
                candidates.append(sig)
                log(
                    f"  ✔ {pair:20s} pump={sig['pump_pct']:+.1f}% "
                    f"vol×{sig['vol_ratio']:.1f} RSI={sig['rsi']:.1f}",
                    "info",
                )
            # Sleep ada di dalam analyze_pair — tidak perlu di sini

        except Exception as e:
            log(f"  [{pair}] unexpected error: {e}", "warn")

    log(
        f"\nScan selesai: {scanned} pair diperiksa | "
        f"{fetched} candle di-fetch | "
        f"{len(candidates)} sinyal ditemukan"
    )

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
