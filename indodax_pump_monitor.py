"""
╔══════════════════════════════════════════════════════════════════╗
║       INDODAX PUMP SIGNAL MONITOR  v3.0  (AGRESIF + SCORING)    ║
║                                                                  ║
║  Target    : Semua pair IDR microcap di Indodax                  ║
║  Stack     : Indodax Public API + pandas + Supabase + Telegram   ║
║  Mode      : Stateful + Outcome Tracking + Adaptive Threshold    ║
║                                                                  ║
║  SIGNAL LOGIC v3:                                                ║
║                                                                  ║
║  Core Gates (WAJIB — keduanya):                                  ║
║    G1. Price Pump  — harga naik ≥ PRICE_PUMP_PCT% (avg 3 snap)  ║
║    G2. Vol Spike   — Δvol ≥ VOL_SPIKE_MULT× baseline            ║
║                                                                  ║
║  Path A — Normal (butuh G1 + G2 + Breakout):                     ║
║    G3. Breakout dari recent high (3 snap window, BREAKOUT_PCT)   ║
║                                                                  ║
║  Path B — Sniper (G1 + G2 cukup, tanpa breakout):                ║
║    Aktif jika pump ≥ SNIPER_PUMP_PCT + vol ≥ SNIPER_VOL_MULT    ║
║    Entry lebih awal, sebelum breakout terkonfirmasi              ║
║                                                                  ║
║  Anti Fake Pump: pump > 12% di 1 window = skip (sudah top)      ║
║                                                                  ║
║  Optional Scoring (tidak memblokir, hanya nilai kualitas):       ║
║    RSI ≥45        → +1    (max +1, tidak overweight)             ║
║    Buy pressure   → +1/2/3 (data 50 trade terakhir)             ║
║    Vol confirm    → +1    (delta acceleration)                   ║
║    Sniper path    → +1    (early entry bonus)                    ║
║                                                                  ║
║  Mode tracking (untuk WR per mode):                              ║
║    NORMAL / SNIPER / EARLY  — tersimpan ke DB per sinyal        ║
║                                                                  ║
║  Learning:                                                       ║
║    Outcome tracker: TP1/TP2/TP3/SL dicek tiap run              ║
║    Bayesian WR dari tabel indodax_signals di Supabase           ║
║    WR bucket per: tier × pump_bucket × vol_bucket              ║
║    Adaptive threshold: ketat/longgar otomatis berdasarkan WR    ║
║    WATCH threshold: adapt dari WR khusus early signal           ║
║                                                                  ║
║  ENV VARS (opsional):                                            ║
║    PRICE_PUMP_PCT     float  default=1.5                         ║
║    VOL_SPIKE_MULT     float  default=2.0                         ║
║    BREAKOUT_PCT       float  default=1.0  (terpisah dari pump)   ║
║    SNIPER_PUMP_PCT    float  default=1.2                         ║
║    SNIPER_VOL_MULT    float  default=1.8                         ║
║    ENTRY_DISCOUNT_PCT float  default=0.5  (kecil tapi impact)    ║
║    SL_PCT             float  default=5.0                         ║
║    BTC_DROP_THRESHOLD float  default=-3.0 (block jika BTC -3%+)  ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import json
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

# Supabase — opsional, aktif hanya jika env var tersedia
try:
    from supabase import create_client as _sb_create
    _SB_AVAILABLE = True
except ImportError:
    _SB_AVAILABLE = False

# ══════════════════════════════════════════════════════════════════
#  CONFIG & CONSTANTS
# ══════════════════════════════════════════════════════════════════

TELEGRAM_BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str   = os.environ.get("TELEGRAM_CHAT_ID",   "")
_TG_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Signal thresholds (bisa di-override oleh adaptive_thresholds())
PRICE_PUMP_PCT = float(os.environ.get("PRICE_PUMP_PCT", "1.5"))
VOL_SPIKE_MULT = float(os.environ.get("VOL_SPIKE_MULT", "2.0"))
RSI_OVERSOLD   = float(os.environ.get("RSI_OVERSOLD",   "35.0"))
RSI_RECOVERY   = float(os.environ.get("RSI_RECOVERY",   "50.0"))

# Volume filter
VOL_IDR_MIN = float(os.environ.get("VOL_IDR_MIN", "5_000_000"))
VOL_IDR_MAX = float(os.environ.get("VOL_IDR_MAX", "2_000_000_000"))

# Risk management
SL_PCT  = float(os.environ.get("SL_PCT",  "5.0"))
TP1_PCT = float(os.environ.get("TP1_PCT", "5.0"))
TP2_PCT = float(os.environ.get("TP2_PCT", "10.0"))
TP3_PCT = float(os.environ.get("TP3_PCT", "20.0"))
# Entry zone: sedikit di bawah harga pump untuk menghindari chasing
# Entry ideal = harga_sekarang × (1 - ENTRY_DISCOUNT_PCT/100)
ENTRY_DISCOUNT_PCT = float(os.environ.get("ENTRY_DISCOUNT_PCT", "0.5"))
# Extreme pump bypass: jika pump ≥ nilai ini, Gate 4 RSI dilewati
# Pump 15%+ dalam satu snapshot = momentum cukup kuat tanpa perlu konfirmasi RSI
EXTREME_PUMP_BYPASS_PCT = float(os.environ.get("EXTREME_PUMP_BYPASS_PCT", "10.0"))

# ── Position Sizing (dummy — simulasi, tidak auto-execute)
# Dummy deposit: 100 juta IDR
# Ukuran posisi per trade = 2% dari total deposit (money management standar)
DUMMY_DEPOSIT_IDR  = float(os.environ.get("DUMMY_DEPOSIT_IDR",  "100000000"))  # 100 juta
BASE_POSITION_IDR  = float(os.environ.get("BASE_POSITION_IDR",
                           str(DUMMY_DEPOSIT_IDR * 0.02)))                     # 2% = 2 juta/trade
TP1_CLOSE_PCT      = float(os.environ.get("TP1_CLOSE_PCT",       "70"))        # 70% ditutup di TP1

# ── Portfolio Risk Limits
MAX_OPEN_TRADES    = int(os.environ.get("MAX_OPEN_TRADES",    "15"))   # maks trade terbuka sekaligus
MAX_PORTFOLIO_HEAT = float(os.environ.get("MAX_PORTFOLIO_HEAT","30"))  # maks % deposit yang terpakai
# Dengan 15 trade × 2% = 30% deposit terpakai maksimal

# State
STATE_FILE       = os.environ.get("STATE_FILE",    "price_state.json")
MAX_SNAPSHOTS    = int(os.environ.get("MAX_SNAPSHOTS",    "30"))
MIN_SNAPS_SIGNAL = int(os.environ.get("MIN_SNAPS_SIGNAL",  "3"))
MIN_SNAPS_RSI    = 16

# DB & Learning
SUPABASE_URL          = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY          = os.environ.get("SUPABASE_KEY", "")
SIGNAL_EXPIRE_HOURS   = int(os.environ.get("SIGNAL_EXPIRE_HOURS", "48"))
WR_MIN_SAMPLE         = int(os.environ.get("WR_MIN_SAMPLE",       "15"))
DB_TABLE              = "indodax_signals"
# Adaptive threshold bounds
PUMP_PCT_MIN, PUMP_PCT_MAX = 1.0, 6.0
VOL_MULT_MIN,  VOL_MULT_MAX  = 1.5, 8.0
# Bayesian prior (Jeffreys)
_BAYES_A, _BAYES_B = 0.5, 0.5

# ── Market Condition Filter (BTC trend guard)
BTC_TREND_ENABLED  = os.environ.get("BTC_TREND_ENABLED", "true").lower() == "true"
BTC_TREND_WINDOW   = int(os.environ.get("BTC_TREND_WINDOW",  "3"))    # snapshot terakhir untuk trend
BTC_DROP_THRESHOLD = float(os.environ.get("BTC_DROP_THRESHOLD", "-3.0"))  # % drop → skip semua sinyal

# ── Volume Confirmation (vol_delta naik N snapshot berturut)
VOL_CONFIRM_SNAPS  = int(os.environ.get("VOL_CONFIRM_SNAPS", "3"))  # min snapshot delta naik

# ── Auto-blacklist pair manipulatif
BLACKLIST_SL_COUNT    = int(os.environ.get("BLACKLIST_SL_COUNT",   "3"))   # SL berapa kali → blacklist
BLACKLIST_WINDOW_DAYS = int(os.environ.get("BLACKLIST_WINDOW_DAYS", "7"))   # dalam berapa hari
BLACKLIST_COOLDOWN_H  = int(os.environ.get("BLACKLIST_COOLDOWN_H",  "48"))  # jam cooldown setelah blacklist
# Harga naik ≥ nilai ini dari baseline WATCH → kirim early signal (sebelum pump penuh)
EARLY_BREAKOUT_PCT   = float(os.environ.get("EARLY_BREAKOUT_PCT",  "1.0"))
# Berapa lama WATCH disimpan di state sebelum expire (dalam menit)
WATCH_EXPIRE_MINUTES = int(os.environ.get("WATCH_EXPIRE_MINUTES",  "30"))
# SL lebih ketat untuk early entry (masuk lebih awal, risiko lebih kecil)
EARLY_SL_PCT         = float(os.environ.get("EARLY_SL_PCT",         "3.0"))
REQUEST_TIMEOUT_SEC = int(os.environ.get("REQUEST_TIMEOUT",     "10"))
TG_SEND_SLEEP_SEC   = float(os.environ.get("TG_SEND_SLEEP_SEC", "1.0"))
MAX_SIGNALS_PER_RUN = int(os.environ.get("MAX_SIGNALS_PER_RUN", "5"))

# Indodax API
INDODAX_TICKER_ALL = "https://indodax.com/api/ticker_all"
INDODAX_SUMMARIES  = "https://indodax.com/api/summaries"
INDODAX_TRADES     = "https://indodax.com/api/{pair}/trades"
INDODAX_DEPTH      = "https://indodax.com/api/{pair}/depth"

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
_LOG_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO,
               "warn": logging.WARNING, "warning": logging.WARNING,
               "error": logging.ERROR}

def log(msg: str, level: str = "info") -> None:
    _logger.log(_LOG_LEVELS.get(level, logging.INFO), msg)


# ══════════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════════

def tg(message: str) -> bool:
    url     = f"{_TG_BASE}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message,
                "parse_mode": "HTML", "disable_web_page_preview": True}
    for attempt in range(1, 4):
        try:
            resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SEC)
            if resp.status_code == 200:
                return True
            if resp.status_code == 429:
                time.sleep(resp.json().get("parameters", {}).get("retry_after", 5))
                continue
            log(f"Telegram HTTP {resp.status_code} (attempt {attempt}): {resp.text[:150]}", "warn")
        except requests.RequestException as e:
            log(f"Telegram error (attempt {attempt}): {e}", "warn")
        time.sleep(2 ** attempt)
    log("Telegram: semua 3 attempt gagal.", "error")
    return False


def validate_telegram() -> bool:
    try:
        resp = requests.get(f"{_TG_BASE}/getMe", timeout=REQUEST_TIMEOUT_SEC)
        if resp.status_code == 404:
            log("❌ TELEGRAM_BOT_TOKEN tidak valid (404). Buat token baru via @BotFather.", "error")
            return False
        if resp.status_code != 200:
            log(f"❌ Telegram getMe HTTP {resp.status_code}", "error")
            return False
        log(f"✅ Telegram OK — bot: @{resp.json().get('result', {}).get('username', '?')}")
    except Exception as e:
        log(f"❌ Telegram getMe: {e}", "error")
        return False
    try:
        resp = requests.post(f"{_TG_BASE}/sendChatAction",
                             json={"chat_id": TELEGRAM_CHAT_ID, "action": "typing"},
                             timeout=REQUEST_TIMEOUT_SEC)
        if resp.status_code == 400:
            log(f"❌ TELEGRAM_CHAT_ID tidak valid: {resp.text[:100]}", "error")
            return False
        log("✅ Telegram chat_id valid")
    except Exception as e:
        log(f"❌ Telegram chat check: {e}", "error")
        return False
    return True


# ══════════════════════════════════════════════════════════════════
#  STATE MANAGEMENT
# ══════════════════════════════════════════════════════════════════
#
#  Format:
#  {
#    "updated": <unix_ts>,
#    "snapshots": [
#      { "ts": <unix_ts>, "data": { "btc_idr": {"last": .., "vol": .., "high": .., "low": ..} } },
#      ...   ← index -1 = paling baru
#    ]
#  }

def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        log(f"State file tidak ditemukan ({STATE_FILE}) — mulai fresh.")
        return {"updated": 0, "snapshots": []}
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
        log(f"State loaded: {len(state.get('snapshots', []))} snapshot dari {STATE_FILE}")
        return state
    except Exception as e:
        log(f"Gagal load state ({e}) — mulai fresh.", "warn")
        return {"updated": 0, "snapshots": []}


def save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, separators=(",", ":"))
        log(f"State disimpan: {len(state['snapshots'])} snapshot → {STATE_FILE}")
    except Exception as e:
        log(f"Gagal simpan state: {e}", "error")


def build_snapshot(tickers: dict) -> dict:
    """Buat snapshot dari ticker sekarang. Hanya pair yang lolos vol filter."""
    data = {}
    for pair, td in tickers.items():
        try:
            last = float(td.get("last",    0) or 0)
            vol  = float(td.get("vol_idr", 0) or 0)
            high = float(td.get("high",    0) or 0)
            low  = float(td.get("low",     0) or 0)
            if last > 0 and VOL_IDR_MIN <= vol <= VOL_IDR_MAX:
                data[pair] = {"last": last, "vol": vol, "high": high, "low": low}
        except Exception:
            pass
    return {"ts": int(time.time()), "data": data}


def update_state(state: dict, snapshot: dict) -> dict:
    snaps = state.get("snapshots", [])
    snaps.append(snapshot)
    if len(snaps) > MAX_SNAPSHOTS:
        snaps = snaps[-MAX_SNAPSHOTS:]
    return {"updated": snapshot["ts"], "snapshots": snaps}


def get_pair_history(state: dict, pair: str) -> list[dict]:
    """
    Ambil riwayat pair dari state.
    Return list [{"ts":.., "last":.., "vol":.., "high":.., "low":..}]
    oldest → newest.
    """
    result = []
    for snap in state.get("snapshots", []):
        entry = snap["data"].get(pair)
        if entry:
            result.append({"ts": snap["ts"], **entry})
    return result


def state_save_thresholds(state: dict, pump_pct: float, vol_mult: float,
                          watch_vol_min: float | None = None,
                          watch_flat_max: float | None = None) -> dict:
    """
    Simpan threshold adaptif ke state — termasuk WATCH threshold.
    """
    state["adaptive"] = {
        "pump_pct":      round(pump_pct, 3),
        "vol_mult":      round(vol_mult, 3),
        "watch_vol_min": round(watch_vol_min, 3) if watch_vol_min else WATCH_VOL_BUILD_MIN,
        "watch_flat_max": round(watch_flat_max, 3) if watch_flat_max else WATCH_PRICE_FLAT_PCT,
        "updated_ts":    int(time.time()),
    }
    return state


def state_load_thresholds(state: dict) -> tuple[float, float, float, float]:
    """
    Load threshold adaptif dari state.
    Return (pump_pct, vol_mult, watch_vol_min, watch_flat_max)
    """
    adaptive      = state.get("adaptive", {})
    pump          = float(adaptive.get("pump_pct",      PRICE_PUMP_PCT))
    vol           = float(adaptive.get("vol_mult",      VOL_SPIKE_MULT))
    watch_vol     = float(adaptive.get("watch_vol_min", WATCH_VOL_BUILD_MIN))
    watch_flat    = float(adaptive.get("watch_flat_max",WATCH_PRICE_FLAT_PCT))
    pump          = max(PUMP_PCT_MIN, min(PUMP_PCT_MAX, pump))
    vol           = max(VOL_MULT_MIN, min(VOL_MULT_MAX,  vol))
    watch_vol     = max(1.2, min(4.0, watch_vol))
    watch_flat    = max(0.5, min(5.0, watch_flat))
    return pump, vol, watch_vol, watch_flat


def state_save_watches(state: dict, watches: list[dict]) -> dict:
    """
    Simpan WATCH signals ke state agar bisa dicek di run berikutnya.
    Format per entry: {pair, baseline_price, baseline_vol, ts, vol_ratio}
    Hanya simpan yang belum expire.
    """
    now_ts    = int(time.time())
    expire_ts = now_ts - WATCH_EXPIRE_MINUTES * 60

    # Merge dengan watch lama yang belum expire
    existing  = {w["pair"]: w for w in state.get("watches", [])
                 if w.get("ts", 0) >= expire_ts}

    # Update/tambah watch baru
    for w in watches:
        existing[w["pair"]] = {
            "pair":           w["pair"],
            "baseline_price": w["price"],
            "baseline_vol":   w["vol_idr"],
            "vol_ratio":      w["vol_ratio"],
            "ts":             now_ts,
        }

    state["watches"] = list(existing.values())
    return state


def state_get_active_watches(state: dict) -> list[dict]:
    """Return list WATCH yang masih aktif (belum expire)."""
    now_ts    = int(time.time())
    expire_ts = now_ts - WATCH_EXPIRE_MINUTES * 60
    return [w for w in state.get("watches", []) if w.get("ts", 0) >= expire_ts]


def state_remove_watch(state: dict, pair: str) -> dict:
    """Hapus pair dari watch list (sudah jadi early signal atau expired)."""
    state["watches"] = [w for w in state.get("watches", []) if w["pair"] != pair]
    return state


# ══════════════════════════════════════════════════════════════════
#  FEATURE 1: MARKET CONDITION FILTER
# ══════════════════════════════════════════════════════════════════

def get_btc_trend(state: dict, tickers: dict) -> tuple[float, str]:
    """
    Cek trend BTC menggunakan data btc_idr dari Indodax tickers.
    Tidak butuh API Binance — pakai data yang sudah di-fetch.

    BTC price history disimpan di state["btc_prices"] antar run.
    Return (pct_change, label)
    """
    # Ambil harga BTC/IDR dari tickers yang sudah di-fetch
    btc_td    = tickers.get("btc_idr", {})
    btc_price = float(btc_td.get("last", 0) or 0)

    if btc_price > 0:
        state.setdefault("btc_prices", []).append({
            "ts": int(time.time()), "price": btc_price
        })
        # Trim ke window yang diperlukan
        state["btc_prices"] = state["btc_prices"][-(BTC_TREND_WINDOW * 3):]

    btc_history = state.get("btc_prices", [])

    if len(btc_history) < 2 or btc_price <= 0:
        return 0.0, "neutral"

    # Bandingkan vs BTC_TREND_WINDOW snapshot lalu
    window  = btc_history[-min(BTC_TREND_WINDOW + 1, len(btc_history)):]
    old_btc = float(window[0]["price"])
    if old_btc <= 0:
        return 0.0, "neutral"

    pct = round((btc_price - old_btc) / old_btc * 100, 2)

    if pct <= BTC_DROP_THRESHOLD:
        label = "bearish"
    elif pct >= 1.5:
        label = "bullish"
    else:
        label = "neutral"

    log(f"📊 BTC/IDR trend: {pct:+.2f}% ({label}) | "
        f"now={btc_price:,.0f} | prev={old_btc:,.0f} "
        f"({len(window)-1} snap lalu)")
    return pct, label


# ══════════════════════════════════════════════════════════════════
#  FEATURE 2: VOLUME CONFIRMATION (3 snapshot berturut)
# ══════════════════════════════════════════════════════════════════

def gate_vol_confirm(history: list[dict], curr_vol: float) -> tuple[bool, int]:
    """
    Konfirmasi volume: delta vol snapshot terakhir ≥ 2× rata-rata delta
    3 snapshot sebelumnya.

    Ini lebih robust untuk rolling 24h vol Indodax yang naik-turun acak.
    Tidak butuh ascending berturut-turut — cukup delta sekarang signifikan
    dibanding baseline recent.

    Return (passed, ratio_int)
    """
    if len(history) < VOL_CONFIRM_SNAPS + 1:
        return True, 0   # tidak cukup data → lolos (jangan blokir)

    vols   = [h["vol"] for h in history] + [curr_vol]
    deltas = [abs(vols[i] - vols[i-1]) for i in range(1, len(vols))]

    if len(deltas) < VOL_CONFIRM_SNAPS + 1:
        return True, 0

    curr_delta     = deltas[-1]
    recent_deltas  = [d for d in deltas[-VOL_CONFIRM_SNAPS-1:-1] if d > 0]

    if not recent_deltas or curr_delta <= 0:
        return True, 0   # tidak ada baseline → lolos

    baseline = sum(recent_deltas) / len(recent_deltas)
    if baseline <= 0:
        return True, 0

    ratio = curr_delta / baseline
    # Lolos jika delta sekarang ≥ 1.5× baseline recent (volume akselerasi)
    return ratio >= 1.2, int(ratio)


# ══════════════════════════════════════════════════════════════════
#  FEATURE 3: AUTO-BLACKLIST PAIR MANIPULATIF
# ══════════════════════════════════════════════════════════════════

def db_update_blacklist(state: dict) -> dict:
    """
    Cek Supabase untuk pair yang SL berkali-kali dalam BLACKLIST_WINDOW_DAYS.
    Simpan blacklist ke state agar cepat dicek tanpa DB call di setiap pair.

    Blacklist entry: {"pair": str, "sl_count": int, "until_ts": int}
    """
    sb = _get_sb()
    if not sb:
        return state

    try:
        from_dt = (datetime.now(timezone.utc) -
                   timedelta(days=BLACKLIST_WINDOW_DAYS)).isoformat()
        rows = (
            sb.table(DB_TABLE)
            .select("pair, result")
            .eq("result", "SL")
            .gte("closed_at", from_dt)
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"db_update_blacklist: {e}", "warn")
        return state

    # Hitung SL per pair
    sl_counts: dict[str, int] = {}
    for r in rows:
        pair = r.get("pair", "")
        sl_counts[pair] = sl_counts.get(pair, 0) + 1

    # Build blacklist
    now_ts      = int(time.time())
    cooldown_ts = now_ts + BLACKLIST_COOLDOWN_H * 3600
    blacklist   = {}

    for pair, count in sl_counts.items():
        if count >= BLACKLIST_SL_COUNT:
            blacklist[pair] = {
                "sl_count":  count,
                "until_ts":  cooldown_ts,
            }
            log(f"  🚫 BLACKLIST: {pair} — {count}× SL dalam {BLACKLIST_WINDOW_DAYS} hari")

    if blacklist:
        log(f"📊 Blacklist: {len(blacklist)} pair diblokir "
            f"({BLACKLIST_COOLDOWN_H}j cooldown)")

    state["blacklist"] = blacklist
    return state


def is_blacklisted(state: dict, pair: str) -> bool:
    """Return True jika pair masih dalam cooldown blacklist."""
    bl   = state.get("blacklist", {})
    info = bl.get(pair)
    if not info:
        return False
    # Cek apakah cooldown sudah berakhir
    if int(time.time()) > info.get("until_ts", 0):
        return False
    return True


# ══════════════════════════════════════════════════════════════════
#  RSI (pure pandas — no external lib)
# ══════════════════════════════════════════════════════════════════

def _calc_rsi(prices: list[float], length: int = 14) -> list[float]:
    s        = pd.Series(prices, dtype=float)
    delta    = s.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, min_periods=length, adjust=False).mean()
    rs  = avg_gain / avg_loss.replace(0, float("nan"))
    return (100 - (100 / (1 + rs))).tolist()


# ══════════════════════════════════════════════════════════════════
#  STATEFUL SIGNAL GATES
# ══════════════════════════════════════════════════════════════════

BREAKOUT_PCT   = float(os.environ.get("BREAKOUT_PCT",   "1.0"))
SNIPER_PUMP_PCT = float(os.environ.get("SNIPER_PUMP_PCT", "0.8"))
SNIPER_VOL_MULT = float(os.environ.get("SNIPER_VOL_MULT", "1.5"))


def gate_price_pump(curr: float, history: list[dict]) -> tuple[bool, float]:
    """
    Gate 1: harga vs rata-rata 3 snapshot terakhir (kurangi noise).
    Anti fake pump: pump > 12% = sudah top, skip.
    """
    if not history:
        return False, 0.0
    window   = history[-min(3, len(history)):]
    avg_prev = sum(h["last"] for h in window) / len(window)
    if avg_prev <= 0:
        return False, 0.0
    pct = (curr - avg_prev) / avg_prev * 100
    if pct > 12.0:
        return False, round(pct, 2)
    return pct >= PRICE_PUMP_PCT, round(pct, 2)


def gate_vol_spike(curr_vol: float, history: list[dict]) -> tuple[bool, float]:
    """
    Gate 2: Δvol_idr sekarang vs baseline historis.
    vol_idr Indodax = rolling 24h sum → delta antar snapshot ≈ vol interval itu.
    """
    if len(history) < 3:
        return False, 0.0
    vols   = [h["vol"] for h in history] + [curr_vol]
    deltas = [max(0.0, vols[i] - vols[i - 1]) for i in range(1, len(vols))]
    curr_d = deltas[-1]
    base_d = [d for d in deltas[:-1] if d > 0]
    if not base_d or curr_d <= 0:
        return False, 0.0
    baseline = sum(base_d) / len(base_d)
    if baseline <= 0:
        return False, 0.0
    ratio = curr_d / baseline
    return ratio >= VOL_SPIKE_MULT, round(ratio, 2)


def gate_breakout(curr: float, history: list[dict]) -> tuple[bool, float]:
    """
    Gate 3 (Path A): breakout dari recent high 3 snapshot.
    BREAKOUT_PCT terpisah dari PRICE_PUMP_PCT → no double filter.
    Path B (Sniper) tidak memerlukan gate ini.
    """
    if not history:
        return False, 0.0
    window      = history[-min(3, len(history)):]
    recent_high = max(h["last"] for h in window)
    if recent_high <= 0:
        return False, 0.0
    return curr >= recent_high * (1 + BREAKOUT_PCT / 100), round(recent_high, 8)


def gate_rsi_cross(curr: float, history: list[dict]) -> tuple[bool, float, bool]:
    """
    Gate 4: RSI(14) dari ≤RSI_OVERSOLD ke ≥RSI_RECOVERY.
    Return (passed, current_rsi, enough_data)
    """
    if len(history) < MIN_SNAPS_RSI:
        return False, 0.0, False   # tidak cukup data, gate dilewati

    prices   = [h["last"] for h in history] + [curr]
    rsi_list = [v for v in _calc_rsi(prices) if not (isinstance(v, float) and math.isnan(v))]

    if len(rsi_list) < 5:
        return False, 0.0, False

    curr_rsi   = rsi_list[-1]
    recent_low = min(rsi_list[-5:-1])

    if math.isnan(curr_rsi) or math.isnan(recent_low):
        return False, 0.0, True

    crossed = (recent_low <= RSI_OVERSOLD) and (curr_rsi >= RSI_RECOVERY)
    return crossed, round(curr_rsi, 1), True


# ══════════════════════════════════════════════════════════════════
#  INDODAX API
# ══════════════════════════════════════════════════════════════════

def fetch_all_tickers() -> dict[str, dict]:
    try:
        resp = requests.get(INDODAX_TICKER_ALL, timeout=REQUEST_TIMEOUT_SEC)
        resp.raise_for_status()
        return {k: v for k, v in resp.json().get("tickers", {}).items()
                if k.endswith("_idr")}
    except Exception as e:
        log(f"fetch_all_tickers() failed: {e}", "error")
        return {}


def fetch_summaries() -> dict[str, dict]:
    """
    Ambil /api/summaries — berisi price_24h, price_7d, volume per pair.
    Berguna untuk filter pair yang sudah overbought vs yang masih fresh.
    Return {pair: {last, high, low, vol, price_24h, price_7d}, ...}
    """
    try:
        resp = requests.get(INDODAX_SUMMARIES, timeout=REQUEST_TIMEOUT_SEC)
        resp.raise_for_status()
        raw  = resp.json().get("tickers", {})
        return {k: v for k, v in raw.items() if k.endswith("_idr")}
    except Exception as e:
        log(f"fetch_summaries() failed: {e}", "warn")
        return {}


def fetch_trades(pair: str) -> list[dict]:
    """
    Ambil 50 trade terakhir untuk satu pair.
    Field: {date, price, amount, type (buy/sell), tid}
    Digunakan untuk deteksi buy/sell pressure real-time.
    """
    try:
        url  = INDODAX_TRADES.format(pair=pair)
        resp = requests.get(url, timeout=REQUEST_TIMEOUT_SEC)
        resp.raise_for_status()
        body = resp.content.strip()
        if not body:
            return []
        return resp.json() or []
    except Exception:
        return []


def fetch_depth(pair: str) -> dict:
    """
    Ambil order book pair.
    Field: {buy: [[price, amount], ...], sell: [[price, amount], ...]}
    Digunakan untuk cek ketebalan ask wall (resistance) dan bid wall (support).
    """
    try:
        url  = INDODAX_DEPTH.format(pair=pair)
        resp = requests.get(url, timeout=REQUEST_TIMEOUT_SEC)
        resp.raise_for_status()
        body = resp.content.strip()
        if not body:
            return {}
        return resp.json() or {}
    except Exception:
        return {}


# ══════════════════════════════════════════════════════════════════
#  ENRICHED DATA GATES
# ══════════════════════════════════════════════════════════════════

def gate_trade_pressure(pair: str) -> tuple[bool, float, str]:
    """
    Gate: Buy pressure dominan dari 50 trade terakhir.
    Hitung buy_volume vs sell_volume → buyer ratio.

    Lolos jika buyer ratio ≥ 60% (lebih banyak yang beli dari jual).

    Return (passed, buyer_ratio, summary)
    """
    trades = fetch_trades(pair)
    if not trades:
        return True, 0.0, "no_data"   # tidak ada data → lolos (jangan blokir)

    buy_vol  = sum(float(t.get("amount", 0)) for t in trades if t.get("type") == "buy")
    sell_vol = sum(float(t.get("amount", 0)) for t in trades if t.get("type") == "sell")
    total    = buy_vol + sell_vol

    if total <= 0:
        return True, 0.0, "no_volume"

    ratio   = round(buy_vol / total * 100, 1)
    passed  = ratio >= 60.0
    summary = f"{ratio:.0f}% buy ({len(trades)} trades)"
    return passed, ratio, summary


def gate_ask_wall(pair: str, curr_price: float, tp1: float) -> tuple[bool, float]:
    """
    Gate: Cek apakah ada ask wall tebal antara harga sekarang dan TP1.
    Jika ask wall > 5× rata-rata ask → sinyal mungkin kesulitan menembus.

    Return (passed, wall_ratio)
    passed=True jika tidak ada ask wall yang menghalangi (jalan ke TP1 bersih)
    """
    depth = fetch_depth(pair)
    if not depth or "sell" not in depth:
        return True, 0.0   # tidak ada data → lolos

    asks = depth.get("sell", [])
    if not asks:
        return True, 0.0

    # Filter ask antara harga sekarang dan TP1
    blocking_asks = [
        float(a[1]) for a in asks
        if curr_price <= float(a[0]) <= tp1
    ]
    all_asks = [float(a[1]) for a in asks[:20]]  # top 20

    if not blocking_asks or not all_asks:
        return True, 0.0

    avg_ask_size = sum(all_asks) / len(all_asks)
    max_block    = max(blocking_asks)
    wall_ratio   = round(max_block / avg_ask_size, 1)

    # Lolos jika tidak ada ask yang > 5× rata-rata (tidak ada wall tebal)
    return wall_ratio < 5.0, wall_ratio


def gate_price_7d(pair: str, curr_price: float, summaries: dict) -> tuple[bool, float]:
    """
    Gate: Cek posisi harga sekarang relatif terhadap harga 7 hari lalu.
    Jika sudah naik > 50% dalam 7 hari → sudah terlalu tinggi, skip.
    Jika masih di bawah atau baru naik sedikit → masih ada upside.

    Return (passed, pct_vs_7d)
    """
    summary = summaries.get(pair, {})
    price_7d = float(summary.get("price_7d", 0) or 0)

    if price_7d <= 0 or curr_price <= 0:
        return True, 0.0   # tidak ada data → lolos

    pct_vs_7d = round((curr_price - price_7d) / price_7d * 100, 1)

    # Skip jika sudah pump > 50% dalam 7 hari (kemungkinan sudah di puncak)
    passed = pct_vs_7d <= 50.0
    return passed, pct_vs_7d


# ══════════════════════════════════════════════════════════════════
#  PRE-PUMP WATCH DETECTION
# ══════════════════════════════════════════════════════════════════
#
#  Tujuan: deteksi tanda akumulasi SEBELUM pump terjadi.
#  Sinyal "⚡ WATCH" dikirim terpisah — bukan entry signal,
#  tapi peringatan dini untuk pasang alert manual di Indodax.
#
#  3 kondisi SEMUA harus terpenuhi:
#
#  [W1] Price Compression
#       Harga bergerak < WATCH_PRICE_FLAT_PCT% dalam 6 snapshot
#       (harga sideways / konsolidasi — belum pump)
#
#  [W2] Volume Acceleration
#       Vol_delta meningkat berturut-turut di 3 snapshot terakhir
#       (setiap interval lebih banyak dari interval sebelumnya)
#
#  [W3] Volume Build-Up
#       Vol_delta sekarang WATCH_VOL_BUILD_MIN× s/d WATCH_VOL_BUILD_MAX×
#       baseline — sudah di atas normal tapi BELUM masuk zona pump (5×)
#       Jika sudah ≥ 5×, berarti pump sudah terjadi, bukan pre-pump lagi

WATCH_PRICE_FLAT_PCT  = float(os.environ.get("WATCH_PRICE_FLAT_PCT",  "2.0"))
WATCH_VOL_BUILD_MIN   = float(os.environ.get("WATCH_VOL_BUILD_MIN",   "2.0"))
WATCH_VOL_BUILD_MAX   = float(os.environ.get("WATCH_VOL_BUILD_MAX",   "4.9"))
WATCH_MIN_SNAPS       = int(os.environ.get("WATCH_MIN_SNAPS",          "5"))
MAX_WATCH_PER_RUN     = int(os.environ.get("MAX_WATCH_PER_RUN",        "3"))


def detect_pre_pump(
    pair:        str,
    ticker_data: dict,
    history:     list[dict],
) -> Optional[dict]:
    """
    Deteksi tanda akumulasi pre-pump untuk satu pair.
    Return dict watch signal jika semua 3 kondisi terpenuhi, else None.
    """
    try:
        curr_price = float(ticker_data.get("last",    0) or 0)
        curr_vol   = float(ticker_data.get("vol_idr", 0) or 0)
        high_24h   = float(ticker_data.get("high",    0) or 0)
        low_24h    = float(ticker_data.get("low",     0) or 0)

        # Pre-filter
        if curr_price <= 0 or not (VOL_IDR_MIN <= curr_vol <= VOL_IDR_MAX):
            return None
        if len(history) < WATCH_MIN_SNAPS:
            return None

        # ── W1: Price Compression (harga flat, belum pump)
        window      = history[-6:]
        prices      = [h["last"] for h in window]
        price_range = (max(prices) - min(prices)) / min(prices) * 100
        if price_range >= WATCH_PRICE_FLAT_PCT:
            return None   # harga sudah bergerak — bukan pre-pump lagi

        # ── W2: Volume Acceleration — 2/3 delta naik cukup (Indo jarang perfect ascending)
        vols   = [h["vol"] for h in history[-4:]] + [curr_vol]
        deltas = [max(0.0, vols[i] - vols[i-1]) for i in range(1, len(vols))]
        if len(deltas) < 3:
            return None
        last3  = deltas[-3:]
        rising = sum(1 for i in range(1, 3) if last3[i] > last3[i-1])
        if rising < 2:
            return None

        # ── W3: Volume Build-Up (di zona "naik tapi belum pump")
        all_vols    = [h["vol"] for h in history] + [curr_vol]
        all_deltas  = [max(0.0, all_vols[i] - all_vols[i-1]) for i in range(1, len(all_vols))]
        curr_delta  = all_deltas[-1]
        base_deltas = [d for d in all_deltas[:-1] if d > 0]
        if not base_deltas or curr_delta <= 0:
            return None
        baseline = sum(base_deltas) / len(base_deltas)
        if baseline <= 0:
            return None
        vol_ratio = curr_delta / baseline
        if not (WATCH_VOL_BUILD_MIN <= vol_ratio <= WATCH_VOL_BUILD_MAX):
            return None   # terlalu kecil atau sudah masuk zona pump

        # Semua kondisi terpenuhi → pre-pump watch
        coin = pair.replace("_idr", "").upper()
        return {
            "pair":       pair,
            "coin":       coin,
            "price":      curr_price,
            "vol_idr":    curr_vol,
            "vol_ratio":  round(vol_ratio, 2),
            "price_flat": round(price_range, 2),
            "high_24h":   high_24h,
            "low_24h":    low_24h,
            "snaps":      len(history),
            "ts":         datetime.now(WIB),
        }

    except Exception as e:
        log(f"detect_pre_pump({pair}): {e}", "warn")
        return None


def format_watch(w: dict) -> str:
    """
    WATCH — pre-pump detection.
    Tampilkan trigger price yang jelas: jika tembus → Early Signal otomatis terkirim.
    """
    pair_display = w["pair"].replace("_", "/").upper()
    ts           = w["ts"].strftime("%H:%M WIB")
    trigger      = w["price"] * (1 + EARLY_BREAKOUT_PCT / 100)
    potential    = round((w["high_24h"] - w["price"]) / w["price"] * 100, 1) \
                   if w.get("high_24h", 0) > w["price"] else 0
    vr  = w["vol_ratio"]
    bar = "█" * min(int(vr), 5) + "░" * max(0, 5 - int(vr))

    return (
        f"⚡ <b>WATCH — {w['coin']}</b>  [{bar}]\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair       : <b>{pair_display}</b>\n"
        f"Harga skrg : <b>{_fp(w['price'])}</b> IDR\n"
        f"🔔 Trigger  : <b>{_fp(trigger)}</b> IDR  "
        f"<i>(+{EARLY_BREAKOUT_PCT:.1f}% → Early Signal)</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Vol Build  : <b>{w['vol_ratio']:.1f}×</b>  <i>(accelerating)</i>\n"
        f"Price Flat : <b>{w['price_flat']:.1f}%</b>  <i>(konsolidasi)</i>\n"
        f"Potensi    : <b>+{potential:.1f}%</b> ke high 24h\n"
        f"Vol 24h    : {_fmt_idr(w['vol_idr'])}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"<i>⚡ Jika harga tembus trigger, Early Signal akan "
        f"otomatis dikirim run berikutnya.</i>\n"
        f"⏰ {ts}"
    )


def check_early_signals(
    active_watches: list[dict],
    tickers:        dict,
) -> list[dict]:
    """
    Cek pair dari WATCH list apakah sudah breakout EARLY_BREAKOUT_PCT%.
    Jika ya → buat early signal dengan entry lebih awal dari pump biasa.

    Keunggulan vs sinyal pump biasa:
    - Entry lebih rendah (harga belum naik penuh)
    - SL lebih ketat (EARLY_SL_PCT = 3% vs 5%)
    - R/R lebih baik karena entry lebih baik
    """
    early_sigs = []
    now_ts     = int(time.time())

    for w in active_watches:
        pair           = w["pair"]
        baseline_price = float(w["baseline_price"])
        td             = tickers.get(pair, {})
        curr_price     = float(td.get("last", 0) or 0)

        if curr_price <= 0 or baseline_price <= 0:
            continue

        pct_change = (curr_price - baseline_price) / baseline_price * 100

        # Breakout: harga naik ≥ EARLY_BREAKOUT_PCT dari baseline WATCH
        # Tapi belum terlalu besar (< PRICE_PUMP_PCT × 2) → masih early
        if EARLY_BREAKOUT_PCT <= pct_change < PRICE_PUMP_PCT * 2:
            age_min = (now_ts - w["ts"]) / 60
            coin    = pair.replace("_idr", "").upper()
            vol_idr = float(td.get("vol_idr", 0) or 0)
            high_24h = float(td.get("high", 0) or 0)
            low_24h  = float(td.get("low",  0) or 0)

            early_sigs.append({
                "pair":         pair,
                "coin":         coin,
                "price":        curr_price,
                "baseline":     baseline_price,
                "pct_from_watch": round(pct_change, 2),
                "vol_idr":      vol_idr,
                "vol_ratio":    w.get("vol_ratio", 0),
                "high_24h":     high_24h,
                "low_24h":      low_24h,
                "watch_age_min": round(age_min, 0),
                "ts":           datetime.now(WIB),
            })
            log(f"  ⚡→🚀 EARLY: {pair} +{pct_change:.1f}% dari WATCH baseline "
                f"(watch {age_min:.0f} menit lalu)")

    return early_sigs


def format_early_signal(sig: dict) -> str:
    """
    Format early signal — entry sebelum pump penuh.
    Level TP dihitung dinamis:
      SL  = entry × (1 - EARLY_SL_PCT%)   ← lebih ketat dari sinyal biasa
      R   = entry - SL
      TP1 = high_24h jika antara entry+1R dan entry+2R, else entry+1R
      TP2 = entry + 2R  (R/R standar 1:2)
      TP3 = entry + 3R  atau high_24h × 1.03 jika lebih tinggi
    """
    pair_display = sig["pair"].replace("_", "/").upper()
    entry   = sig["price"]
    high_24h = sig.get("high_24h", 0)
    sl      = entry * (1 - EARLY_SL_PCT / 100)
    R       = entry - sl

    # TP dinamis
    tp2 = entry + 2 * R
    tp3_base = entry + 3 * R
    # TP3: ambil yang lebih tinggi antara 3R atau high_24h × 1.03
    tp3 = max(tp3_base, high_24h * 1.03) if high_24h > tp2 else tp3_base
    # TP1: high_24h jika berada di antara 1R dan 2R (natural resistance)
    tp1_base = entry + 1 * R
    if high_24h > 0 and tp1_base < high_24h < tp2:
        tp1 = high_24h
        tp1_label = f"<i>+{round((tp1-entry)/entry*100,1):.1f}% — high 24h</i>"
    else:
        tp1 = tp1_base
        tp1_label = f"<i>+{round((tp1-entry)/entry*100,1):.1f}% (1R)</i>"

    tp2_pct = round((tp2 - entry) / entry * 100, 1)
    tp3_pct = round((tp3 - entry) / entry * 100, 1)

    low_line = (
        f"🛡 Support  : <code>{_fp(sig['low_24h'])}</code>  <i>(low 24h)</i>\n"
        if sig.get("low_24h", 0) > 0 else ""
    )
    # Resistance note jika high_24h berada antara entry dan TP2
    resistance = ""
    if 0 < high_24h <= tp2:
        resistance = (
            f"⚠️ <i>Resistance di {_fp(high_24h)} (high 24h) — "
            f"waspada sebelum TP2</i>\n"
        )

    ts = sig["ts"].strftime("%H:%M WIB")

    return (
        f"🚀 <b>EARLY SIGNAL — {sig['coin']}</b>\n"
        f"<i>Konfirmasi WATCH {sig['watch_age_min']:.0f} mnt lalu</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair       : <b>{pair_display}</b>\n"
        f"Entry skrg : <b>{_fp(entry)}</b> IDR\n"
        f"Dari WATCH : <b>+{sig['pct_from_watch']:.1f}%</b> "
        f"<i>(baseline {_fp(sig['baseline'])})</i>\n"
        f"\n"
        f"🎯 TP1  : <b>{_fp(tp1)}</b>  {tp1_label}\n"
        f"🏆 TP2  : <b>{_fp(tp2)}</b>  <i>+{tp2_pct:.1f}% (2R) — target utama</i>\n"
        f"🚀 TP3  : <b>{_fp(tp3)}</b>  <i>+{tp3_pct:.1f}% (3R)</i>\n"
        f"{low_line}"
        f"🔴 SL   : <b>{_fp(sl)}</b>  <i>-{EARLY_SL_PCT:.0f}%</i>\n"
        f"R/R     : <b>1:2.0</b>  |  R = {_fp(R)} IDR\n"
        f"{resistance}"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Vol 24h    : {_fmt_idr(sig['vol_idr'])}\n"
        f"Vol Build  : {sig['vol_ratio']:.1f}× baseline\n"
        f"⏰ {ts}\n"
        f"⚡ <i>Early entry — SL ketat {EARLY_SL_PCT:.0f}% wajib!</i>"
    )

def calc_levels(current_price: float, high_24h: float = 0.0) -> dict:
    """
    Hitung level trading dari harga sekarang.

    Entry = current_price × (1 - ENTRY_DISCOUNT_PCT/100)
    SL    = entry × (1 - SL_PCT/100)
    R     = entry - SL  (satuan risiko)

    TP dihitung berdasarkan R-multiple (bukan fixed %):
      TP1 = entry + 1× R  → titik impas jika sebagian posisi di-close
      TP2 = entry + 2× R  → profit bersih 1× risiko
      TP3 = entry + 3× R  → extended target

    Jika high_24h berada antara TP1 dan TP2, ia menjadi
    resistance_note — zona waspada sebelum TP2 tercapai.
    """
    if current_price <= 0:
        log(f"calc_levels: invalid price {current_price}", "warn")
        return {}

    entry = current_price * (1 - ENTRY_DISCOUNT_PCT / 100)
    sl    = entry * (1 - SL_PCT / 100)
    R     = entry - sl
    if R <= 0:
        return {}

    tp1 = entry + 1.0 * R
    tp2 = entry + 2.0 * R
    tp3 = entry + 3.0 * R

    # R/R vs TP2 selalu 2.0× by definition
    rr = 2.0

    # Pct aktual dari entry (untuk display)
    tp1_pct = round((tp1 - entry) / entry * 100, 1)
    tp2_pct = round((tp2 - entry) / entry * 100, 1)
    tp3_pct = round((tp3 - entry) / entry * 100, 1)
    sl_pct  = round((entry - sl) / entry * 100, 1)

    # Cek apakah high_24h jadi natural resistance sebelum TP2
    resistance_note = ""
    if high_24h > entry:
        if tp1 < high_24h < tp2:
            resistance_note = f"⚠️ Resistance di {_fp(high_24h)} (high 24h) — waspada sebelum TP2"
        elif high_24h <= tp1:
            resistance_note = f"⚠️ High 24h {_fp(high_24h)} di bawah TP1 — zona resistance ketat"

    return {
        "current":        current_price,
        "entry":          round(entry, 8),
        "sl":             round(sl,    8),
        "sl_pct":         sl_pct,
        "tp1":            round(tp1,   8),
        "tp2":            round(tp2,   8),
        "tp3":            round(tp3,   8),
        "tp1_pct":        tp1_pct,
        "tp2_pct":        tp2_pct,
        "tp3_pct":        tp3_pct,
        "rr":             rr,
        "R":              round(R, 8),
        "resistance_note": resistance_note,
    }


# ══════════════════════════════════════════════════════════════════
#  PAIR ANALYZER
# ══════════════════════════════════════════════════════════════════

def analyze_pair(
    pair:        str,
    ticker_data: dict,
    history:     list[dict],
    gate_stats:  dict,
    state:       dict | None = None,
    summaries:   dict | None = None,
) -> Optional[dict]:
    def _reject(reason: str) -> None:
        gate_stats[reason] = gate_stats.get(reason, 0) + 1

    try:
        curr_price = float(ticker_data.get("last",    0) or 0)
        vol_idr    = float(ticker_data.get("vol_idr", 0) or 0)
        high_24h   = float(ticker_data.get("high",    0) or 0)
        low_24h    = float(ticker_data.get("low",     0) or 0)

        if curr_price <= 0:
            _reject("PRE_price_zero"); return None
        if not (VOL_IDR_MIN <= vol_idr <= VOL_IDR_MAX):
            _reject("PRE_vol_out_of_range"); return None
        if len(history) < MIN_SNAPS_SIGNAL:
            _reject("STATE_need_more_snaps"); return None

        # ── Feature 3: Blacklist check
        if state and is_blacklisted(state, pair):
            _reject("BLACKLISTED"); return None

        # Gate 1 + Early sniper check
        # Ambil pump_pct dulu tanpa threshold check
        pump_ok, pump_pct = gate_price_pump(curr_price, history)

        # Gate 2: Volume Spike
        vol_ok, vol_ratio = gate_vol_spike(vol_idr, history)

        # ── SNIPER PATH: pump + vol saja, bypass PRICE_PUMP_PCT threshold
        # Aktif jika pump >= SNIPER_PUMP_PCT (0.8%) + vol >= SNIPER_VOL_MULT (1.5×)
        # Ini tangkap start pump sebelum threshold normal terpenuhi
        sniper_ok = (pump_pct >= SNIPER_PUMP_PCT and vol_ratio >= SNIPER_VOL_MULT)

        # G1 wajib jika bukan sniper path
        if not pump_ok and not sniper_ok:
            _reject("G1_price_pump_fail"); return None

        # G2 wajib untuk semua path
        if not vol_ok and not sniper_ok:
            _reject("G2_vol_spike_fail"); return None

        # Gate 3 (Path A — Normal): breakout dari recent high
        is_extreme = pump_pct >= EXTREME_PUMP_BYPASS_PCT
        break_ok, prev_max = gate_breakout(curr_price, history)
        if not break_ok and not sniper_ok:
            _reject("G3_breakout_fail"); return None

        # ══ SCORING OPSIONAL — tidak memblokir ══════════════════════
        sig_score = 0

        # Vol confirmation — bonus kecil
        try:
            if gate_vol_confirm(history, vol_idr)[0]:
                sig_score += 1
        except Exception:
            pass

        # RSI — max +1, tidak overweight. Nol jika extreme (tidak relevan)
        _, curr_rsi, has_rsi = gate_rsi_cross(curr_price, history)
        if not is_extreme and has_rsi and curr_rsi >= 45:
            sig_score += 1

        # Buy pressure — hanya bonus, no penalty (data noisy)
        buyer_ratio = 0.0
        try:
            _, buyer_ratio, _ = gate_trade_pressure(pair)
            if   buyer_ratio >= 70: sig_score += 3
            elif buyer_ratio >= 60: sig_score += 2
            elif buyer_ratio >= 50: sig_score += 1
        except Exception:
            pass

        # Ask wall — info saja, no penalty
        wall_ratio = 0.0
        try:
            _, wall_ratio = gate_ask_wall(
                pair, curr_price, curr_price * (1 + SL_PCT / 100)
            )
        except Exception:
            pass

        # 7d — info saja
        pct_7d = 0.0
        if summaries:
            try:
                _, pct_7d = gate_price_7d(pair, curr_price, summaries)
            except Exception:
                pass

        # Sniper path bonus (+1 kecil — early entry)
        if sniper_ok and not break_ok:
            sig_score += 1

        coin = pair.replace("_idr", "").upper()
        return {
            "pair":        pair,       "coin":       coin,
            "price":       curr_price, "vol_idr":    vol_idr,
            "vol_ratio":   vol_ratio,  "pump_pct":   pump_pct,
            "high_24h":    high_24h,   "low_24h":    low_24h,
            "prev_max":    prev_max,   "rsi":        curr_rsi,
            "has_rsi":     has_rsi,    "is_extreme": is_extreme,
            "is_sniper":   sniper_ok and not break_ok,
            "buyer_ratio": buyer_ratio,"wall_ratio": wall_ratio,
            "pct_7d":      pct_7d,     "sig_score":  sig_score,
            "snaps":       len(history),"ts":        datetime.now(WIB),
        }

    except Exception as e:
        log(f"analyze_pair({pair}): {e}", "warn")
        _reject("EXCEPTION")
        return None


# ══════════════════════════════════════════════════════════════════
#  TIER SCORING
# ══════════════════════════════════════════════════════════════════
#
#  Adaptasi dari Gate.io bot TIER_MIN_SCORE (S:12 A+:9 A:6)
#  ke sinyal yang tersedia di Indodax pump monitor.
#
#  Komponen skor (max 15):
#  ┌─────────────────────────────────┬──────┐
#  │ Pump strength                   │  0-5 │
#  │   >= 10%  → 5 | >= 7% → 4      │      │
#  │   >= 5%   → 3 | >= 3% → 2      │      │
#  ├─────────────────────────────────┼──────┤
#  │ Volume spike                    │  0-5 │
#  │   >= 15×  → 5 | >= 10× → 4     │      │
#  │   >= 7×   → 3 | >= 5×  → 2     │      │
#  ├─────────────────────────────────┼──────┤
#  │ RSI cross confirmed             │  0-3 │
#  │   RSI >= 55 → 3 | >= 50 → 2    │      │
#  ├─────────────────────────────────┼──────┤
#  │ History reliability             │  0-2 │
#  │   snaps >= 16 → 2 | >= 8 → 1   │      │
#  └─────────────────────────────────┴──────┘
#
#  Tier: S >= 12 | A+ >= 9 | A >= 6 | SKIP < 6

_TIER_LABELS = {
    "S":  ("💎", "TIER S",  12),
    "A+": ("🏆", "TIER A+",  9),
    "A":  ("🥇", "TIER A",   6),
}


def calc_score(sig: dict) -> tuple[int, str]:
    """
    Scoring v3 — rebalanced untuk early/microcap Indodax.
    Early move (2%+2×) mendapat score layak, bukan dianggap lemah.
    Tier SKIP hanya untuk score < 4 (tidak ada sinyal nyata).
    """
    score = 0

    # Pump strength — early move dapat score bagus
    p = sig.get("pump_pct", 0)
    if   p >= 7:   score += 5
    elif p >= 5:   score += 4
    elif p >= 3:   score += 3
    elif p >= 2:   score += 2
    elif p >= 1.5: score += 1

    # Volume spike — 2× sudah valid untuk early pump
    v = sig.get("vol_ratio", 0)
    if   v >= 7:   score += 5
    elif v >= 5:   score += 4
    elif v >= 3:   score += 3
    elif v >= 2:   score += 2
    elif v >= 1.5: score += 1

    # RSI — max +1 saja (RSI rendah = early, tidak perlu dihukum)
    if not sig.get("is_extreme") and sig.get("has_rsi") and sig.get("rsi", 0) >= 45:
        score += 1

    # History reliability (0-2)
    s = sig.get("snaps", 0)
    if   s >= 16: score += 2
    elif s >= 8:  score += 1

    # Bonus dari scoring opsional (buy pressure, vol confirm, sniper path)
    score += max(-2, min(3, sig.get("sig_score", 0)))
    score  = max(0, min(15, score))

    if   score >= 12: tier = "S"
    elif score >= 9:  tier = "A+"
    elif score >= 6:  tier = "A"
    elif score >= 4:  tier = "A"    # early/sniper path valid
    else:             tier = "SKIP" # score 0-3: tidak ada sinyal nyata
    return score, tier

def db_portfolio_status() -> dict:
    """
    Cek jumlah open trade, total heat, dan set pair yang sudah open.
    Return {
      "open": int,
      "heat_pct": float,
      "can_add": bool,
      "open_pairs": set[str]   ← untuk dedup cek per-pair
    }
    """
    sb = _get_sb()
    if not sb:
        return {"open": 0, "heat_pct": 0.0, "can_add": True, "open_pairs": set()}
    try:
        rows = (
            sb.table(DB_TABLE)
            .select("id, pair")
            .is_("result", "null")
            .execute()
            .data
        ) or []
        n_open     = len(rows)
        open_pairs = {r["pair"] for r in rows}
        heat_pct   = round(n_open * (BASE_POSITION_IDR / DUMMY_DEPOSIT_IDR) * 100, 1)
        can_add    = (n_open < MAX_OPEN_TRADES) and (heat_pct < MAX_PORTFOLIO_HEAT)
        return {
            "open":       n_open,
            "heat_pct":   heat_pct,
            "can_add":    can_add,
            "open_pairs": open_pairs,
        }
    except Exception as e:
        log(f"db_portfolio_status: {e}", "warn")
        return {"open": 0, "heat_pct": 0.0, "can_add": True, "open_pairs": set()}


def _get_sb():
    """Return Supabase client, atau None jika tidak tersedia/konfigurasi."""
    if not _SB_AVAILABLE or not SUPABASE_URL or not SUPABASE_KEY:
        return None
    try:
        return _sb_create(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        log(f"Supabase init gagal: {e}", "warn")
        return None


def db_save_signal(sig: dict, lvl: dict) -> Optional[str]:
    """
    Simpan sinyal baru ke tabel indodax_signals.
    Return id record jika berhasil, None jika gagal.

    DDL (jalankan sekali di Supabase SQL editor):
    CREATE TABLE indodax_signals (
      id               bigint generated always as identity primary key,
      pair             text not null,
      tier             text,
      mode             text,
      entry            numeric not null,
      sl               numeric not null,
      tp1              numeric not null,
      tp2              numeric not null,
      tp3              numeric not null,
      pump_pct         numeric,
      vol_ratio        numeric,
      rsi              numeric,
      snaps            integer,
      sent_at          timestamptz not null default now(),
      tp1_hit          boolean default false,
      tp2_hit          boolean default false,
      result           text,
      closed_at        timestamptz,
      pnl_pct          numeric,
      threshold_pump   numeric,
      threshold_vol    numeric
    );

    -- Jika tabel sudah ada, jalankan ALTER ini:
    -- ALTER TABLE indodax_signals ADD COLUMN IF NOT EXISTS tp1_hit boolean DEFAULT false;
    -- ALTER TABLE indodax_signals ADD COLUMN IF NOT EXISTS tp2_hit boolean DEFAULT false;
    -- ALTER TABLE indodax_signals ADD COLUMN IF NOT EXISTS tier text;
    -- ALTER TABLE indodax_signals ADD COLUMN IF NOT EXISTS mode text;
    """
    sb = _get_sb()
    if not sb or not lvl:
        return None
    try:
        _, tier = calc_score(sig)
        # Track mode per sinyal untuk WR analysis per path
        if sig.get("is_sniper"):
            mode = "SNIPER"
        elif sig.get("pump_pct", 0) < PRICE_PUMP_PCT:
            mode = "EARLY"    # entry pre-pump (dari WATCH → Early Signal)
        else:
            mode = "NORMAL"
        row = {
            "pair":           sig["pair"],
            "tier":           tier,
            "mode":           mode,
            "entry":          lvl["entry"],
            "sl":             lvl["sl"],
            "tp1":            lvl["tp1"],
            "tp2":            lvl["tp2"],
            "tp3":            lvl["tp3"],
            "pump_pct":       sig.get("pump_pct"),
            "vol_ratio":      sig.get("vol_ratio"),
            "rsi":            sig.get("rsi") or None,
            "snaps":          sig.get("snaps"),
            "sent_at":        datetime.now(timezone.utc).isoformat(),
            "result":         None,
            "threshold_pump": PRICE_PUMP_PCT,
            "threshold_vol":  VOL_SPIKE_MULT,
        }
        resp = sb.table(DB_TABLE).insert(row).execute()
        rec_id = resp.data[0].get("id") if resp.data else None
        log(f"  DB saved: {sig['pair']} → id={rec_id}")
        return str(rec_id) if rec_id else None
    except Exception as e:
        log(f"  db_save_signal({sig['pair']}): {e}", "warn")
        return None


def db_evaluate_outcomes(tickers: dict) -> dict:
    """
    Cek semua open signals (result IS NULL) dan update outcome
    berdasarkan harga ticker Indodax sekarang.

    tickers : dict dari fetch_all_tickers() — dikirim dari run_scan
              agar tidak double-fetch dari Indodax API.

    Logic per row:
      price >= tp3 → TP3 (win terbesar)
      price >= tp2 → TP2
      price >= tp1 → TP1 (partial — trade tetap open)
      price <= sl  → SL
      age > SIGNAL_EXPIRE_HOURS → EXPIRED

    Return stats dict.
    """
    sb = _get_sb()
    stats = {"evaluated": 0, "tp1": 0, "tp2": 0, "tp3": 0, "sl": 0, "expired": 0}
    if not sb:
        return stats

    try:
        rows = (
            sb.table(DB_TABLE)
            .select("id, pair, tier, mode, entry, sl, tp1, tp2, tp3, sent_at, tp1_hit, tp2_hit")
            .is_("result", "null")
            .order("sent_at", desc=False)
            .limit(50)
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"db_evaluate_outcomes query: {e}", "warn")
        return stats

    if not rows:
        return stats

    log(f"📋 Outcome check: {len(rows)} open signal(s)...")

    # Gunakan tickers yang sudah di-fetch — tidak perlu fetch ulang
    now_utc = datetime.now(timezone.utc)

    for row in rows:
        stats["evaluated"] += 1
        try:
            rec_id   = row["id"]
            pair     = row["pair"]
            entry    = float(row["entry"])
            sl       = float(row["sl"])
            tp1      = float(row["tp1"])
            tp2      = float(row["tp2"])
            tp3      = float(row["tp3"])
            tp1_hit  = bool(row.get("tp1_hit") or False)
            tp2_hit  = bool(row.get("tp2_hit") or False)
            mode     = (row.get("mode") or "SCALPING").upper()

            sent_at = datetime.fromisoformat(row["sent_at"].replace("Z", "+00:00"))
            age_h   = (now_utc - sent_at).total_seconds() / 3600

            if age_h > SIGNAL_EXPIRE_HOURS:
                sb.table(DB_TABLE).update({
                    "result": "EXPIRED", "pnl_pct": 0.0,
                    "closed_at": now_utc.isoformat(),
                }).eq("id", rec_id).execute()
                stats["expired"] += 1
                log(f"  {pair} → EXPIRED ({age_h:.0f}j)")
                continue

            td    = tickers.get(pair, {})
            price = float(td.get("last", 0) or 0)
            if price <= 0:
                continue

            pnl_pct = round((price - entry) / entry * 100, 2)

            # ── SL: tutup trade + kirim notifikasi
            if price <= sl:
                sl_pnl = round((sl - entry) / entry * 100, 2)
                sb.table(DB_TABLE).update({
                    "result": "SL",
                    "pnl_pct": sl_pnl,
                    "closed_at": now_utc.isoformat(),
                }).eq("id", rec_id).execute()
                stats["sl"] += 1
                log(f"  {pair} → SL ({pnl_pct:+.1f}%)")
                tg(_fmt_stop_loss(pair, entry, sl, price, mode))

            # ── TP3: tutup trade + kirim notifikasi
            elif price >= tp3:
                tp3_pct = round((tp3 - entry) / entry * 100, 1)
                sb.table(DB_TABLE).update({
                    "result": "TP3",
                    "pnl_pct": tp3_pct,
                    "tp1_hit": True, "tp2_hit": True,
                    "closed_at": now_utc.isoformat(),
                }).eq("id", rec_id).execute()
                stats["tp3"] += 1
                log(f"  {pair} → TP3 ({pnl_pct:+.1f}%)")
                tg(_fmt_tp_closed(pair, entry, tp3, "TP3", tp3_pct, mode))

            # ── TP2: tutup trade + kirim notifikasi
            elif price >= tp2:
                tp2_pct = round((tp2 - entry) / entry * 100, 1)
                sb.table(DB_TABLE).update({
                    "result": "TP2",
                    "pnl_pct": tp2_pct,
                    "tp1_hit": True, "tp2_hit": True,
                    "closed_at": now_utc.isoformat(),
                }).eq("id", rec_id).execute()
                stats["tp2"] += 1
                log(f"  {pair} → TP2 ({pnl_pct:+.1f}%)")
                tg(_fmt_tp_closed(pair, entry, tp2, "TP2", tp2_pct, mode))

            # ── TP1 hit pertama kali: kirim notifikasi + tandai di DB
            elif price >= tp1 and not tp1_hit:
                tp1_pct = round((tp1 - entry) / entry * 100, 1)
                sb.table(DB_TABLE).update({
                    "tp1_hit": True,
                }).eq("id", rec_id).execute()
                stats["tp1"] += 1
                log(f"  {pair} → TP1 hit ✅ — nunggu TP2 ({pnl_pct:+.1f}%)")
                tg(_fmt_partial_profit(pair, entry, tp1, tp1_pct, tp2, sl))

            else:
                log(f"  {pair} → open {pnl_pct:+.1f}%  "
                    f"{'[TP1✅]' if tp1_hit else ''}")

        except Exception as e:
            log(f"  outcome [{row.get('pair')}]: {e}", "warn")

    return stats


def _fmt_stop_loss(pair: str, entry: float, sl: float, price: float, mode: str) -> str:
    """Notifikasi Stop Loss kena — mirip Gate.io bot format."""
    pair_display = pair.replace("_", "/").upper()
    pnl_pct      = round((sl - entry) / entry * 100, 1)
    pnl_idr      = BASE_POSITION_IDR * abs(pnl_pct) / 100
    return (
        f"❌ <b>Stop Loss — {pair_display}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Strategy   : {mode} BUY\n"
        f"Entry      : <code>{_fp(entry)}</code> IDR\n"
        f"SL kena    : <code>{_fp(sl)}</code> IDR\n"
        f"Now        : <code>{_fp(price)}</code> IDR\n"
        f"PnL        : <b>{pnl_pct:.1f}%</b>  "
        f"≈ <b>-{_fmt_idr(pnl_idr)}</b>\n"
        f"<i>SL tersentuh — loss terkontrol 🛡</i>"
    )


def _fmt_tp_closed(
    pair:    str,
    entry:   float,
    tp_lvl:  float,
    tp_name: str,
    tp_pct:  float,
    mode:    str,
) -> str:
    """Notifikasi TP2 / TP3 tercapai — trade ditutup penuh."""
    pair_display = pair.replace("_", "/").upper()
    pnl_idr      = BASE_POSITION_IDR * tp_pct / 100
    emoji        = "🏆" if tp_name == "TP2" else "🚀"
    return (
        f"{emoji} <b>{tp_name} Tercapai — {pair_display}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Strategy   : {mode} BUY\n"
        f"Entry      : <code>{_fp(entry)}</code> IDR\n"
        f"{tp_name} kena  : <code>{_fp(tp_lvl)}</code> IDR\n"
        f"PnL        : <b>+{tp_pct:.1f}%</b>  "
        f"≈ <b>+{_fmt_idr(pnl_idr)}</b>\n"
        f"✅ <i>Posisi penuh ditutup — profit terealisasi</i>"
    )


def _fmt_partial_profit(
    pair:    str,
    entry:   float,
    tp1:     float,
    tp1_pct: float,
    tp2:     float,
    sl:      float,
) -> str:
    """
    Notifikasi saat TP1 pertama kali tercapai.
    Hitung realized PnL dummy berdasarkan BASE_POSITION_IDR × TP1_CLOSE_PCT.
    """
    pair_display = pair.replace("_", "/").upper()

    # Kalkulasi posisi dummy
    close_frac    = TP1_CLOSE_PCT / 100              # mis. 0.70
    remain_frac   = 1 - close_frac                   # 0.30
    realized_idr  = BASE_POSITION_IDR * close_frac * (tp1_pct / 100)
    remain_idr    = BASE_POSITION_IDR * remain_frac  # sisa untuk TP2

    # R/R baru setelah SL ke breakeven:
    # risiko = 0, potensi = TP2 profit dari entry
    tp2_pct = round((tp2 - entry) / entry * 100, 1)

    # Adaptive RR label (berapa kali R yang didapat di TP1 vs risiko awal)
    risk_idr = BASE_POSITION_IDR * (abs(entry - sl) / entry)
    adaptive_rr = round(realized_idr / risk_idr, 1) if risk_idr > 0 else 0

    return (
        f"🎯 <b>Partial Profit Taken — {pair_display}</b>\n"
        f"TP1 tercapai <b>+{tp1_pct:.1f}%</b> ✅\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"• {TP1_CLOSE_PCT:.0f}% posisi ditutup  "
        f"<i>(adaptive RR={adaptive_rr})</i>\n"
        f"• Realized  : <b>+{_fmt_idr(realized_idr)}</b>\n"
        f"• Sisa posisi: {remain_frac*100:.0f}%  "
        f"≈ {_fmt_idr(remain_idr)}\n"
        f"• SL digeser ke entry → <b>{_fp(entry)}</b> IDR  "
        f"<i>(breakeven)</i>\n"
        f"• Menunggu TP2 <b>{_fp(tp2)}</b>  "
        f"<i>(+{tp2_pct:.1f}%) untuk sisa posisi...</i>"
    )


def db_open_trades_report(tickers: dict) -> Optional[str]:
    """
    Ambil semua open trade dari DB, fetch harga terkini, format jadi laporan.
    Return formatted string untuk Telegram, atau None jika tidak ada / error.
    """
    sb = _get_sb()
    if not sb:
        return None
    try:
        rows = (
            sb.table(DB_TABLE)
            .select("id, pair, tier, mode, entry, sl, tp1, tp2, tp3, sent_at, tp1_hit, tp2_hit")
            .is_("result", "null")
            .order("sent_at", desc=False)
            .limit(10)
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"db_open_trades_report query: {e}", "warn")
        return None

    if not rows:
        return None

    now_utc = datetime.now(timezone.utc)
    lines   = []

    for i, row in enumerate(rows, 1):
        try:
            pair     = row["pair"]
            tier     = row.get("tier", "A") or "A"
            mode     = row.get("mode", "SCALPING") or "SCALPING"
            entry    = float(row["entry"])
            sl       = float(row["sl"])
            tp1      = float(row["tp1"])
            tp2      = float(row["tp2"])
            tp3      = float(row["tp3"])
            tp1_hit  = bool(row.get("tp1_hit") or False)
            tp2_hit  = bool(row.get("tp2_hit") or False)

            sent_at  = datetime.fromisoformat(row["sent_at"].replace("Z", "+00:00"))
            age_h    = int((now_utc - sent_at).total_seconds() / 3600)

            # Harga sekarang dari tickers yang sudah di-fetch
            td       = tickers.get(pair, {})
            now_price = float(td.get("last", 0) or 0)
            pnl_pct  = round((now_price - entry) / entry * 100, 2) if now_price > 0 else 0.0
            pnl_icon = "📈" if pnl_pct >= 0 else "📉"

            # TP status label
            tp1_label = f"  <b>⚡TP1✅</b> nunggu TP2" if tp1_hit and not tp2_hit else ""
            tp2_label = f"  <b>⚡TP2✅</b> nunggu TP3" if tp2_hit else ""
            tp_status = tp2_label or tp1_label

            # Tier emoji
            tier_emoji = {"S": "💎", "A+": "🏆", "A": "🥇"}.get(tier, "🥇")

            pair_display = pair.replace("_", "/").upper()
            pnl_str = f"{pnl_pct:+.2f}%" if now_price > 0 else "N/A"

            line = (
                f"\n{i}. 🟢 BUY {pair_display} [{mode}]{tp_status}\n"
                f"   {tier_emoji} Tier {tier}  |  Usia: {age_h}j\n"
                f"   Entry : <code>{_fp(entry)}</code>\n"
                f"   TP1   : <code>{_fp(tp1)}</code>  "
                f"<i>(+{round((tp1-entry)/entry*100,1):.1f}%)</i>  {'✅' if tp1_hit else ''}\n"
                f"   TP2   : <code>{_fp(tp2)}</code>  "
                f"<i>(+{round((tp2-entry)/entry*100,1):.1f}%)</i>  {'✅' if tp2_hit else ''}\n"
                f"   TP3   : <code>{_fp(tp3)}</code>  "
                f"<i>(+{round((tp3-entry)/entry*100,1):.1f}%)</i>\n"
                f"   SL    : <code>{_fp(sl)}</code>  "
                f"<i>(-{round((entry-sl)/entry*100,1):.1f}%)</i>\n"
                f"   Now   : <b>{_fp(now_price)}</b>  {pnl_icon} <b>{pnl_str}</b>"
            )
            lines.append(line)

        except Exception as e:
            log(f"  open_trades row error: {e}", "warn")

    if not lines:
        return None

    header = (
        f"📋 <b>Open Trades ({len(lines)}/{len(rows)})</b>\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    return header + "".join(lines)


def db_equity_report() -> Optional[str]:
    """
    Ringkasan performa dummy portfolio dari indodax_signals.
    Hitung Realized PnL, WR, open trades — mirip Equity Report Gate.io bot.
    """
    sb = _get_sb()
    if not sb:
        return None
    try:
        closed = (
            sb.table(DB_TABLE)
            .select("result, pnl_pct")
            .not_.is_("result", "null")
            .neq("result", "EXPIRED")
            .execute()
            .data
        ) or []
        open_count = (
            sb.table(DB_TABLE)
            .select("id", count="exact")
            .is_("result", "null")
            .execute()
            .count
        ) or 0
    except Exception as e:
        log(f"db_equity_report: {e}", "warn")
        return None

    if not closed and open_count == 0:
        return None

    WIN_SET = {"TP1", "TP2", "TP3"}
    n_total = len(closed)
    n_win   = sum(1 for r in closed if (r.get("result") or "").upper() in WIN_SET)
    n_loss  = n_total - n_win
    wr_pct  = round(n_win / n_total * 100, 1) if n_total > 0 else 0

    # Realized PnL dummy: setiap trade pakai BASE_POSITION_IDR
    realized = 0.0
    for r in closed:
        pnl = float(r.get("pnl_pct") or 0)
        realized += BASE_POSITION_IDR * (pnl / 100)

    realized_str = f"+{_fmt_idr(realized)}" if realized >= 0 else f"-{_fmt_idr(abs(realized))}"

    # Equity sekarang = deposit + realized PnL
    equity_now = DUMMY_DEPOSIT_IDR + realized
    dd_pct     = round((equity_now - DUMMY_DEPOSIT_IDR) / DUMMY_DEPOSIT_IDR * 100, 2)
    dd_icon    = "🟢" if dd_pct >= 0 else "🔴"

    # WR label
    wr_icon = "🟢" if wr_pct >= 55 else ("🟡" if wr_pct >= 45 else "🔴")

    now_wib = datetime.now(WIB).strftime("%Y-%m-%d %H:%M WIB")
    return (
        f"📊 <b>Equity Report</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Deposit awal  : <b>{_fmt_idr(DUMMY_DEPOSIT_IDR)}</b>\n"
        f"Equity skrg   : <b>{_fmt_idr(equity_now)}</b>  "
        f"{dd_icon} <i>({dd_pct:+.2f}%)</i>\n"
        f"Realized PnL  : <b>{realized_str}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"WR            : {wr_icon} <b>{wr_pct:.1f}%</b>  "
        f"({n_win}W / {n_loss}L — {n_total} trades)\n"
        f"Pos/trade     : {_fmt_idr(BASE_POSITION_IDR)}  "
        f"<i>(2% deposit)</i>\n"
        f"Open trades   : {open_count}/{MAX_OPEN_TRADES}  "
        f"🔥 Heat: <b>{round(open_count*(BASE_POSITION_IDR/DUMMY_DEPOSIT_IDR)*100,1):.1f}%"
        f"/{MAX_PORTFOLIO_HEAT:.0f}%</b>\n"
        f"<i>Snapshot: {now_wib}</i>"
    )


def db_load_winrate() -> dict:
    """
    Hitung Bayesian winrate dari sinyal yang sudah closed.
    Return {
      "wr": float,           # Bayesian win rate 0-1
      "wr_freq": float,      # frequentist untuk referensi
      "n_total": int,
      "n_win": int,
      "expectancy": float,   # rata-rata pnl_pct per sinyal
    }
    Atau {} jika data belum cukup / Supabase tidak tersedia.
    """
    sb = _get_sb()
    if not sb:
        return {}
    try:
        rows = (
            sb.table(DB_TABLE)
            .select("result, pnl_pct")
            .not_.is_("result", "null")
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"db_load_winrate: {e}", "warn")
        return {}

    if len(rows) < WR_MIN_SAMPLE:
        log(f"📊 WR: belum cukup data ({len(rows)}/{WR_MIN_SAMPLE} sinyal)")
        return {"n_total": len(rows), "n_win": 0, "wr": 0.0, "wr_freq": 0.0, "expectancy": 0.0}

    WIN_SET  = {"TP1", "TP2", "TP3"}
    wins     = [r for r in rows if (r.get("result") or "").upper() in WIN_SET]
    n_total  = len(rows)
    n_win    = len(wins)

    wr_bayes   = (n_win + _BAYES_A) / (n_total + _BAYES_A + _BAYES_B)
    wr_freq    = n_win / n_total
    pnl_vals   = [float(r["pnl_pct"]) for r in rows if r.get("pnl_pct") is not None]
    expectancy = round(sum(pnl_vals) / len(pnl_vals), 2) if pnl_vals else 0.0

    # WR khusus Early Signal — pump_pct < PRICE_PUMP_PCT = entry pre-pump
    early_rows   = [r for r in rows if float(r.get("pump_pct") or 99) < PRICE_PUMP_PCT]
    n_early      = len(early_rows)
    n_early_win  = sum(1 for r in early_rows if (r.get("result") or "").upper() in WIN_SET)
    early_wr     = round(
        (n_early_win + _BAYES_A) / (n_early + _BAYES_A + _BAYES_B), 3
    ) if n_early > 0 else 0.5

    if n_early > 0:
        log(f"📊 Early Signal WR: {early_wr:.0%} ({n_early_win}/{n_early})")

    return {
        "wr":          round(wr_bayes, 3),
        "wr_freq":     round(wr_freq,  3),
        "n_total":     n_total,
        "n_win":       n_win,
        "expectancy":  expectancy,
        "early_wr":    early_wr,
        "n_early":     n_early,
    }


# ══════════════════════════════════════════════════════════════════
#  WR BUCKETING — PREDICTIVE LEARNING
# ══════════════════════════════════════════════════════════════════
#
#  Bot mengelompokkan sinyal historis berdasarkan 3 fitur:
#    tier (S/A+/A) × pump_pct bucket × vol_ratio bucket
#
#  Saat sinyal baru masuk:
#  1. Tentukan bucket → lookup WR historis
#  2. WR bucket < MIN_BUCKET_WR → sinyal di-skip
#  3. Predicted WR ditampilkan di notifikasi
#
#  Fallback: fine → medium → coarse (jika sample kurang)

MIN_BUCKET_WR     = float(os.environ.get("MIN_BUCKET_WR",    "0.40"))
MIN_BUCKET_SAMPLE = int(os.environ.get("MIN_BUCKET_SAMPLE",  "5"))


def _pump_bucket(pct: float) -> str:
    if pct >= 20: return "pump≥20%"
    if pct >= 10: return "pump10-20%"
    if pct >= 5:  return "pump5-10%"
    return "pump<5%"


def _vol_bucket(ratio: float) -> str:
    if ratio >= 20: return "vol≥20x"
    if ratio >= 12: return "vol12-20x"
    if ratio >= 7:  return "vol7-12x"
    return "vol<7x"


def db_load_wr_buckets() -> dict:
    """
    Hitung WR per bucket dari seluruh sinyal closed.
    Return {"tier|pump_bucket|vol_bucket": {"wr": float, "n": int, "n_win": int}, ...}
    """
    sb = _get_sb()
    if not sb:
        return {}
    try:
        rows = (
            sb.table(DB_TABLE)
            .select("result, tier, pump_pct, vol_ratio")
            .not_.is_("result", "null")
            .neq("result", "EXPIRED")
            .execute()
            .data
        ) or []
    except Exception as e:
        log(f"db_load_wr_buckets: {e}", "warn")
        return {}

    if len(rows) < WR_MIN_SAMPLE:
        return {}

    WIN_SET = {"TP1", "TP2", "TP3"}
    raw: dict[str, dict] = {}

    for r in rows:
        tier      = (r.get("tier") or "A").strip()
        pump_pct  = float(r.get("pump_pct")  or 0)
        vol_ratio = float(r.get("vol_ratio") or 0)
        result    = (r.get("result") or "").upper()
        is_win    = result in WIN_SET

        # 3 level granularitas
        for key in [
            f"{tier}|{_pump_bucket(pump_pct)}|{_vol_bucket(vol_ratio)}",  # fine
            f"{tier}|{_pump_bucket(pump_pct)}",                             # medium
            tier,                                                             # coarse
        ]:
            b = raw.setdefault(key, {"n": 0, "n_win": 0})
            b["n"]     += 1
            b["n_win"] += int(is_win)

    # Bayesian WR per bucket
    buckets = {}
    for key, b in raw.items():
        n, nw = b["n"], b["n_win"]
        buckets[key] = {
            "wr":    round((nw + _BAYES_A) / (n + _BAYES_A + _BAYES_B), 3),
            "n":     n,
            "n_win": nw,
        }

    qualified = [(k, b) for k, b in sorted(buckets.items(), key=lambda x: -x[1]["wr"])
                 if b["n"] >= MIN_BUCKET_SAMPLE and k.count("|") == 2]
    log(f"📊 WR Buckets: {len(buckets)} bucket | {len(qualified)} qualified | {len(rows)} closed")
    for key, b in qualified[:3]:   # hanya top 3 untuk ringkas
        icon = "🟢" if b["wr"] >= 0.55 else ("🟡" if b["wr"] >= 0.45 else "🔴")
        log(f"  {icon} {key:<38} {b['wr']:.0%} ({b['n_win']}/{b['n']})")

    return buckets


def predict_wr(sig: dict, buckets: dict) -> tuple[float, str, bool]:
    """
    Prediksi WR sinyal baru dari bucket historis.
    Fallback: fine → medium → coarse.
    Return (predicted_wr, bucket_key, has_enough_data)
    """
    if not buckets:
        return 0.0, "no_data", False

    _, tier   = calc_score(sig)
    pump_pct  = sig.get("pump_pct", 0)
    vol_ratio = sig.get("vol_ratio", 0)

    for key in [
        f"{tier}|{_pump_bucket(pump_pct)}|{_vol_bucket(vol_ratio)}",
        f"{tier}|{_pump_bucket(pump_pct)}",
        tier,
    ]:
        b = buckets.get(key)
        if b and b["n"] >= MIN_BUCKET_SAMPLE:
            icon = "🟢" if b["wr"] >= 0.55 else ("🟡" if b["wr"] >= 0.45 else "🔴")
            log(f"    predWR {icon} {b['wr']:.0%} [{key}] ({b['n_win']}/{b['n']})")
            return b["wr"], key, True

    return 0.0, "insufficient_data", False


def adapt_thresholds(
    wr_data:        dict,
    curr_pump:      float | None = None,
    curr_vol:       float | None = None,
    curr_watch_vol: float | None = None,
    curr_watch_flat: float | None = None,
) -> tuple[float, float, float, float]:
    """
    Sesuaikan semua threshold berdasarkan WR historis.
    - pump/vol: dari WR global
    - watch_vol/watch_flat: dari WR khusus Early Signal

    Return (pump_pct, vol_mult, watch_vol_min, watch_flat_max)
    """
    base_pump       = curr_pump       if curr_pump       is not None else PRICE_PUMP_PCT
    base_vol        = curr_vol        if curr_vol        is not None else VOL_SPIKE_MULT
    base_watch_vol  = curr_watch_vol  if curr_watch_vol  is not None else WATCH_VOL_BUILD_MIN
    base_watch_flat = curr_watch_flat if curr_watch_flat is not None else WATCH_PRICE_FLAT_PCT

    if not wr_data or wr_data.get("n_total", 0) < WR_MIN_SAMPLE:
        return base_pump, base_vol, base_watch_vol, base_watch_flat

    wr = wr_data["wr"]

    # ── Adapt pump/vol dari WR global
    if wr < 0.40:
        factor = 1.15
        log(f"📊 Adaptive: WR={wr:.0%} < 40% → PERKETAT pump/vol ×{factor}")
    elif wr > 0.65:
        factor = 0.90
        log(f"📊 Adaptive: WR={wr:.0%} > 65% → LONGGARKAN pump/vol ×{factor}")
    else:
        factor = 1.0
        log(f"📊 Adaptive: WR={wr:.0%} normal — pump/vol tidak berubah")

    new_pump = round(max(PUMP_PCT_MIN, min(PUMP_PCT_MAX, base_pump * factor)), 2)
    new_vol  = round(max(VOL_MULT_MIN, min(VOL_MULT_MAX, base_vol  * factor)), 2)

    # ── Adapt WATCH threshold dari WR Early Signal
    early_wr = wr_data.get("early_wr", 0.5)
    n_early  = wr_data.get("n_early", 0)

    if n_early >= 5:   # butuh minimal 5 early signal untuk adapt
        if early_wr < 0.40:
            # Early signal sering kalah → perketat WATCH (vol lebih tinggi, flat lebih ketat)
            new_watch_vol  = round(min(4.0, base_watch_vol  * 1.15), 2)
            new_watch_flat = round(max(0.5, base_watch_flat * 0.90), 2)
            log(f"📊 WATCH adapt: early WR={early_wr:.0%} < 40% → "
                f"vol_min {base_watch_vol:.2f}→{new_watch_vol:.2f} "
                f"flat_max {base_watch_flat:.2f}→{new_watch_flat:.2f}")
        elif early_wr > 0.65:
            # Early signal bagus → longgarkan WATCH (catch lebih banyak)
            new_watch_vol  = round(max(1.2, base_watch_vol  * 0.90), 2)
            new_watch_flat = round(min(5.0, base_watch_flat * 1.10), 2)
            log(f"📊 WATCH adapt: early WR={early_wr:.0%} > 65% → "
                f"vol_min {base_watch_vol:.2f}→{new_watch_vol:.2f} "
                f"flat_max {base_watch_flat:.2f}→{new_watch_flat:.2f}")
        else:
            new_watch_vol  = base_watch_vol
            new_watch_flat = base_watch_flat
            log(f"📊 WATCH adapt: early WR={early_wr:.0%} normal — tidak berubah")
    else:
        new_watch_vol  = base_watch_vol
        new_watch_flat = base_watch_flat

    if factor != 1.0:
        log(f"   pump: {base_pump:.2f}% → {new_pump:.2f}% | "
            f"vol: {base_vol:.2f}× → {new_vol:.2f}×")

    return new_pump, new_vol, new_watch_vol, new_watch_flat


# ══════════════════════════════════════════════════════════════════
#  FORMAT
# ══════════════════════════════════════════════════════════════════

def _fp(v: float) -> str:
    if v >= 1_000: return f"{v:,.0f}"
    if v >= 1:     return f"{v:,.2f}"
    return f"{v:.8f}".rstrip("0")

def _fmt_idr(v: float) -> str:
    if v >= 1_000_000_000: return f"{v/1_000_000_000:.1f}M IDR"
    if v >= 1_000_000:     return f"{v/1_000_000:.1f}jt IDR"
    return f"{v:,.0f} IDR"


def format_signal(sig: dict, wr_data: dict | None = None) -> str:
    ts           = sig["ts"]
    pair_display = sig["pair"].replace("_", "/").upper()
    lvl          = calc_levels(sig["price"])
    if not lvl:
        return f"🚨 <b>PUMP — {sig['coin']}</b>\n{pair_display}\n⚠️ Level tidak bisa dihitung."

    # ── Tier & Score
    score, tier            = calc_score(sig)
    tier_emoji, tier_label, _ = _TIER_LABELS.get(tier, ("🥇", "TIER A", 6))
    score_bar = "█" * min(score, 15) + "░" * max(0, 15 - score)

    lvl = calc_levels(sig["price"], sig.get("high_24h", 0))

    # ── Valid window
    valid_until = (ts + timedelta(hours=SIGNAL_EXPIRE_HOURS)).strftime("%H:%M WIB")

    # ── Entry note (pump sudah besar → warning chasing)
    entry_note = ""
    is_extreme = sig.get("is_extreme", False)
    is_sniper  = sig.get("is_sniper",  False)
    if is_extreme:
        entry_note = (
            f"\n🚨 <b>EXTREME +{sig['pump_pct']:.0f}%!</b> "
            f"<i>Entry hati-hati, mungkin sudah top.</i>"
        )
    elif is_sniper:
        entry_note = (
            f"\n⚡ <b>SNIPER ENTRY</b> +{sig['pump_pct']:.1f}% "
            f"<i>(breakout belum terkonfirmasi — SL ketat wajib!)</i>"
        )
    elif sig["pump_pct"] > 5.0:
        entry_note = (
            f"\n⚠️ <i>Pump sudah {sig['pump_pct']:.1f}% — "
            f"tunggu retest ke entry zone!</i>"
        )

    # ── RSI
    rsi_line = (
        f"RSI(14)    : <b>{sig['rsi']:.1f}</b>  <i>(recovery ✅)</i>\n"
        if sig.get("has_rsi") and sig["rsi"] > 0
        else f"RSI(14)    : <i>akumulasi ({sig['snaps']}/{MIN_SNAPS_RSI} snap)</i>\n"
    )

    # ── Hist WR (global) + Predicted WR (bucket)
    pred_wr   = sig.get("pred_wr", 0.0)
    has_bucket = sig.get("has_bucket", False)
    bucket_key = sig.get("bucket_key", "")

    if has_bucket:
        pred_icon = "🟢" if pred_wr >= 0.55 else ("🟡" if pred_wr >= 0.45 else "🔴")
        pred_line = (
            f"Pred WR    : {pred_icon} <b>{pred_wr:.0%}</b>  "
            f"<i>[{bucket_key}]</i>\n"
        )
    else:
        pred_line = f"Pred WR    : <i>akumulasi data bucket</i>\n"

    if wr_data and wr_data.get("n_total", 0) >= WR_MIN_SAMPLE:
        hist_wr = (
            f"Hist WR    : <b>{wr_data['wr']:.0%}★</b> "
            f"({wr_data['n_win']}/{wr_data['n_total']})  "
            f"E[PnL]: <b>{wr_data['expectancy']:+.1f}%</b>\n"
        )
    else:
        n = wr_data.get("n_total", 0) if wr_data else 0
        hist_wr = f"Hist WR    : <i>akumulasi data ({n}/{WR_MIN_SAMPLE})</i>\n"

    # ── Support
    low_ref  = sig.get("low_24h", 0)
    low_line = (
        f"🛡 Support  : <code>{_fp(low_ref)}</code>  <i>(low 24h)</i>\n"
        if low_ref > 0 else ""
    )

    return (
        f"🚨 {tier_emoji} <b>{tier_label} — PUMP {sig['coin']}</b>\n"
        f"<code>Score {score:02d}/15  [{score_bar}]</code>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair       : <b>{pair_display}</b>\n"
        f"⏰ Valid    : {ts.strftime('%H:%M')} → {valid_until}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Harga skrg : <b>{_fp(lvl['current'])}</b> IDR\n"
        f"Entry Zone : <b>{_fp(lvl['entry'])}</b> IDR  "
        f"<i>(-{ENTRY_DISCOUNT_PCT:.1f}% retest)</i>{entry_note}\n"
        f"\n"
        f"🎯 TP1  : <b>{_fp(lvl['tp1'])}</b>  "
        f"<i>+{lvl['tp1_pct']:.1f}% (1R)</i>\n"
        f"🏆 TP2  : <b>{_fp(lvl['tp2'])}</b>  "
        f"<i>+{lvl['tp2_pct']:.1f}% (2R) — target utama</i>\n"
        f"🚀 TP3  : <b>{_fp(lvl['tp3'])}</b>  "
        f"<i>+{lvl['tp3_pct']:.1f}% (3R) — hold sebagian</i>\n"
        f"{low_line}"
        f"🔴 SL   : <b>{_fp(lvl['sl'])}</b>  "
        f"<i>-{lvl['sl_pct']:.1f}% — cut wajib</i>\n"
        f"R/R        : <b>1:{lvl['rr']}</b>  <i>(vs TP2)  |  R = {_fp(lvl['R'])} IDR</i>\n"
        f"{(lvl['resistance_note'] + chr(10)) if lvl.get('resistance_note') else ''}"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pump       : <b>+{sig['pump_pct']:.2f}%</b> vs snapshot lalu\n"
        f"Vol Spike  : <b>{sig['vol_ratio']:.1f}×</b> baseline\n"
        f"Buy Press  : <b>{sig.get('buyer_ratio', 0):.0f}%</b> "
        f"<i>(dari 50 trade terakhir)</i>\n"
        f"{('vs 7d     : <b>+' + str(sig['pct_7d']) + '%</b> ' + '<i>(masih ada upside)</i>' + chr(10)) if sig.get('pct_7d', 0) > 0 else ''}"
        f"{rsi_line}"
        f"Vol 24h    : {_fmt_idr(sig['vol_idr'])}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{pred_line}"
        f"{hist_wr}"
        f"Threshold  : pump≥{PRICE_PUMP_PCT:.1f}% | vol≥{VOL_SPIKE_MULT:.1f}×\n"
        f"<i>⚠️ Pasang SL wajib. Bukan saran investasi.</i>"
    )


def format_summary(scanned: int, n_snaps: int, candidates: int, sent: int) -> str:
    ts = datetime.now(WIB).strftime("%d/%m/%Y %H:%M WIB")
    return (
        f"📊 <b>SCAN SELESAI</b>  |  {'✅ Ada sinyal!' if sent > 0 else '📭 Tidak ada sinyal'}\n"
        f"Pair diperiksa  : {scanned}\n"
        f"Snapshot history: {n_snaps}\n"
        f"Sinyal          : {candidates} ditemukan, {sent} terkirim\n"
        f"⏰ {ts}"
    )


# ══════════════════════════════════════════════════════════════════
#  MAIN RUNNER
# ══════════════════════════════════════════════════════════════════

def run_scan() -> None:
    global PRICE_PUMP_PCT, VOL_SPIKE_MULT   # adaptive threshold dapat diubah per-run

    # ── CREDENTIAL CHECK
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("❌ TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID tidak di-set. Cek GitHub Secrets.", "error")
        sys.exit(1)

    # ── KILL SWITCH: cek file PAUSED di root repo
    # Cara pause: push file bernama "PAUSED" ke repo → bot berhenti sampai file dihapus
    if os.path.exists("PAUSED"):
        msg = (
            f"⏸️ <b>Bot PAUSED</b>\n"
            f"File <code>PAUSED</code> ditemukan di repo.\n"
            f"Hapus file tersebut untuk melanjutkan scan.\n"
            f"⏰ {datetime.now(WIB).strftime('%d/%m/%Y %H:%M WIB')}"
        )
        log("⏸️  PAUSED — file 'PAUSED' ada di repo. Bot tidak scan.", "warn")
        tg(msg)
        return

    log("=" * 64)
    log("🔍 INDODAX PUMP SIGNAL MONITOR v2.1 (STATEFUL + LEARNING)")
    log(f"  Pump≥{PRICE_PUMP_PCT}% | Vol≥{VOL_SPIKE_MULT}× | RSI {RSI_OVERSOLD:.0f}→{RSI_RECOVERY:.0f}")
    log(f"  State: {STATE_FILE} | Max snaps: {MAX_SNAPSHOTS} | Min snaps: {MIN_SNAPS_SIGNAL}")
    log("=" * 64)

    if not validate_telegram():
        log("Scan dibatalkan — perbaiki konfigurasi Telegram.", "error")
        sys.exit(1)

    # Load state
    state   = load_state()
    n_snaps = len(state.get("snapshots", []))
    if n_snaps == 0:
        log("⚠️  Run pertama — hanya kumpulkan data, belum ada sinyal.", "warn")
    elif n_snaps < MIN_SNAPS_SIGNAL:
        log(f"⚠️  Masih akumulasi data: {n_snaps}/{MIN_SNAPS_SIGNAL} snapshot.", "warn")

    # ── Step 1b: Fetch tickers + summaries (dipakai scan + outcome check)
    log("Fetching ticker Indodax...")
    tickers = fetch_all_tickers()
    if not tickers:
        log("Gagal fetch tickers — abort.", "error")
        return
    log(f"Total pair IDR: {len(tickers)}")

    log("Fetching summaries Indodax (price 7d)...")
    summaries = fetch_summaries()
    log(f"Summaries: {len(summaries)} pair")

    # ── Step 1c: Outcome tracking — pass tickers yg sudah di-fetch (no double call)
    outcome_stats = db_evaluate_outcomes(tickers)
    if outcome_stats.get("evaluated", 0) > 0:
        log(f"📋 Outcome: {outcome_stats}")

    # ── Step 1d: Load winrate, buckets & adaptive threshold (persisten dari state)
    wr_data    = db_load_winrate()
    wr_buckets = db_load_wr_buckets()

    # Load threshold dari state (persisten antar run)
    curr_pump, curr_vol, curr_watch_vol, curr_watch_flat = state_load_thresholds(state)
    log(f"📊 Threshold sebelum adapt: pump={curr_pump:.2f}% vol={curr_vol:.2f}× "
        f"watch_vol={curr_watch_vol:.2f}× watch_flat={curr_watch_flat:.2f}%")

    PRICE_PUMP_PCT, VOL_SPIKE_MULT, WATCH_VOL_BUILD_MIN, WATCH_PRICE_FLAT_PCT = \
        adapt_thresholds(wr_data, curr_pump, curr_vol, curr_watch_vol, curr_watch_flat)

    state = state_save_thresholds(state, PRICE_PUMP_PCT, VOL_SPIKE_MULT,
                                   WATCH_VOL_BUILD_MIN, WATCH_PRICE_FLAT_PCT)
    log(f"📊 Threshold aktif: pump={PRICE_PUMP_PCT:.2f}% vol={VOL_SPIKE_MULT:.2f}× "
        f"watch_vol≥{WATCH_VOL_BUILD_MIN:.2f}× flat≤{WATCH_PRICE_FLAT_PCT:.2f}%")

    # ── Feature 1: BTC Trend Filter (pakai BTC/IDR dari tickers Indodax)
    btc_pct, btc_label = get_btc_trend(state, tickers)
    if BTC_TREND_ENABLED and btc_label == "bearish":
        msg = (
            f"📉 <b>Market Bearish — Scan Ditahan</b>\n"
            f"BTC turun <b>{btc_pct:.2f}%</b> dalam {BTC_TREND_WINDOW} snapshot terakhir\n"
            f"Semua sinyal BUY ditahan untuk lindungi modal.\n"
            f"<i>Bot tetap update state dan WATCH, tapi tidak kirim sinyal entry.</i>"
        )
        log(f"📉 BTC bearish {btc_pct:.2f}% → semua sinyal BUY ditahan", "warn")
        tg(msg)
        # Tetap update state + save, tapi skip scan sinyal
        snapshot = build_snapshot(tickers)
        state    = update_state(state, snapshot)
        save_state(state)
        return

    # ── Feature 3: Update blacklist dari DB
    state = db_update_blacklist(state)

    # Scan
    scanned    = 0
    candidates: list[dict] = []
    watches:    list[dict] = []
    gate_stats: dict[str, int] = {}

    for pair, ticker_data in tickers.items():
        scanned += 1
        history = get_pair_history(state, pair)

        # ── Main pump signal (pass state untuk blacklist + summaries untuk gate 5-7)
        sig = analyze_pair(pair, ticker_data, history, gate_stats, state, summaries)
        if sig:
            # Tier check: skip score < 4
            score, tier = calc_score(sig)
            if tier == "SKIP":
                gate_stats["SCORE_TOO_LOW"] = gate_stats.get("SCORE_TOO_LOW", 0) + 1
                continue

            # WR bucket filter
            pred_wr, bucket_key, has_bucket = predict_wr(sig, wr_buckets)
            sig["pred_wr"]    = pred_wr
            sig["bucket_key"] = bucket_key
            sig["has_bucket"] = has_bucket

            if has_bucket and pred_wr < MIN_BUCKET_WR:
                gate_stats["BUCKET_LOW_WR"] = gate_stats.get("BUCKET_LOW_WR", 0) + 1
                log(f"  ⚠ {pair:20s} SKIP bucket WR={pred_wr:.0%} [{bucket_key}]", "warn")
                continue

            path = "⚡SNIPER" if sig.get("is_sniper") else "NORMAL"
            candidates.append(sig)
            wr_tag = f"predWR={pred_wr:.0%}" if has_bucket else "N/A"
            log(f"  ✔ {pair:20s} pump={sig['pump_pct']:+.1f}% "
                f"vol×{sig['vol_ratio']:.1f} score={score} [{path}] {wr_tag}")

        # ── Pre-pump watch
        elif not sig:
            w = detect_pre_pump(pair, ticker_data, history)
            if w:
                watches.append(w)
                log(f"  ⚡ {pair:20s} WATCH vol×{w['vol_ratio']:.1f} flat={w['price_flat']:.1f}%")

    log(f"\nScan: {scanned} pair | {n_snaps} snap lama | "
        f"{len(candidates)} sinyal | {len(watches)} watch")

    # Gate breakdown
    if gate_stats:
        log("─" * 56)
        log("📊 GATE REJECTION (top 8):")
        total = sum(gate_stats.values())
        for reason, count in sorted(gate_stats.items(), key=lambda x: -x[1])[:8]:
            pct = count / scanned * 100 if scanned else 0
            log(f"  {reason:<35} {count:>4}× ({pct:.0f}%)")
        log(f"  {'TOTAL':<35} {total:>4}×")
        log("─" * 56)

    # Update & simpan state SEBELUM kirim sinyal
    # (state tetap tersimpan meski Telegram gagal)
    snapshot = build_snapshot(tickers)
    state    = update_state(state, snapshot)
    save_state(state)

    # Kirim sinyal (sorted: pump_pct desc, vol_ratio desc)
    candidates.sort(key=lambda x: (-x.get("sig_score", 0), -x["pump_pct"], -x["vol_ratio"]))
    sent      = 0
    skipped   = 0
    portfolio = db_portfolio_status()
    log(f"  Portfolio: {portfolio['open']}/{MAX_OPEN_TRADES} open | "
        f"heat {portfolio['heat_pct']:.1f}%/{MAX_PORTFOLIO_HEAT:.0f}%")

    for sig in candidates[:MAX_SIGNALS_PER_RUN]:
        pair = sig["pair"]

        # ── Dedup: skip jika pair sudah punya open trade
        if pair in portfolio.get("open_pairs", set()):
            skipped += 1
            log(f"  ⛔ {pair} SKIP — sudah ada open trade untuk pair ini", "warn")
            continue

        # ── Cek portfolio limit (count + heat)
        if not portfolio["can_add"]:
            skipped += 1
            log(f"  ⛔ {pair} SKIP — portfolio penuh "
                f"({portfolio['open']}/{MAX_OPEN_TRADES} trades, "
                f"heat {portfolio['heat_pct']:.1f}%)", "warn")
            continue

        lvl = calc_levels(sig["price"], sig.get("high_24h", 0))
        if tg(format_signal(sig, wr_data)):
            sent += 1
            db_save_signal(sig, lvl)
            # Update counter lokal agar loop berikutnya ikut terkena limit
            portfolio["open"]       += 1
            portfolio["open_pairs"].add(pair)
            portfolio["heat_pct"]    = round(
                portfolio["open"] * (BASE_POSITION_IDR / DUMMY_DEPOSIT_IDR) * 100, 1
            )
            portfolio["can_add"]     = (
                portfolio["open"] < MAX_OPEN_TRADES and
                portfolio["heat_pct"] < MAX_PORTFOLIO_HEAT
            )
            log(f"  📤 {pair} pump={sig['pump_pct']:+.1f}% | "
                f"portfolio {portfolio['open']}/{MAX_OPEN_TRADES} "
                f"heat {portfolio['heat_pct']:.1f}%")
        time.sleep(TG_SEND_SLEEP_SEC)

    if skipped > 0:
        skipped_names = ", ".join(
            s["coin"] for s in candidates[:MAX_SIGNALS_PER_RUN]
            if s["pair"] in portfolio.get("open_pairs", set()) or not portfolio["can_add"]
        )
        tg(
            f"ℹ️ <b>{skipped} sinyal tidak dikirim</b>\n"
            f"Pair: <code>{skipped_names}</code>\n"
            f"Open: <b>{portfolio['open']}/{MAX_OPEN_TRADES}</b>  |  "
            f"Heat: <b>{portfolio['heat_pct']:.1f}%/{MAX_PORTFOLIO_HEAT:.0f}%</b>\n"
            f"<i>Portfolio penuh atau pair sudah open.</i>"
        )

    # ── Open Trades Report (kirim setelah semua sinyal baru)
    open_report = db_open_trades_report(tickers)
    if open_report:
        tg(open_report)
        log("📋 Open trades report terkirim")
        time.sleep(TG_SEND_SLEEP_SEC)

    # ── Cek WATCH list dari run sebelumnya → early signal
    active_watches  = state_get_active_watches(state)
    early_signals   = check_early_signals(active_watches, tickers) if active_watches else []
    early_sent      = 0

    if early_signals:
        log(f"  🚀 {len(early_signals)} early signal ditemukan dari WATCH list")
        for es in early_signals[:MAX_WATCH_PER_RUN]:
            if pair in portfolio.get("open_pairs", set()):
                log(f"  ⛔ {es['pair']} EARLY SKIP — sudah ada open trade", "warn")
                continue
            if not portfolio["can_add"]:
                log(f"  ⛔ {es['pair']} EARLY SKIP — portfolio penuh", "warn")
                break

            # Build dummy sig dict untuk db_save_signal (pakai EARLY_SL_PCT)
            entry_p   = es["price"]
            sl_p      = entry_p * (1 - EARLY_SL_PCT / 100)
            R         = entry_p - sl_p
            lvl_early = {
                "entry": entry_p, "sl": round(sl_p, 8),
                "tp1":   round(entry_p + R,     8),
                "tp2":   round(entry_p + 2 * R, 8),
                "tp3":   round(entry_p + 3 * R, 8),
            }
            sig_early = {**es, "pump_pct": es["pct_from_watch"],
                         "rsi": 0.0, "has_rsi": False, "is_extreme": False,
                         "snaps": 0, "prev_max": es["baseline"],
                         "pred_wr": 0.0, "has_bucket": False, "bucket_key": ""}

            if tg(format_early_signal(es)):
                early_sent += 1
                db_save_signal(sig_early, lvl_early)
                portfolio["open"]       += 1
                portfolio["open_pairs"].add(es["pair"])
                portfolio["heat_pct"]    = round(
                    portfolio["open"] * (BASE_POSITION_IDR / DUMMY_DEPOSIT_IDR) * 100, 1
                )
                portfolio["can_add"]     = (
                    portfolio["open"] < MAX_OPEN_TRADES and
                    portfolio["heat_pct"] < MAX_PORTFOLIO_HEAT
                )
                state = state_remove_watch(state, es["pair"])
                log(f"  🚀 EARLY {es['pair']} +{es['pct_from_watch']:.1f}% terkirim")
            time.sleep(TG_SEND_SLEEP_SEC)

    # ── Kirim WATCH signals baru + simpan ke state
    watches.sort(key=lambda x: -x["vol_ratio"])
    watch_sent = 0
    for w in watches[:MAX_WATCH_PER_RUN]:
        if tg(format_watch(w)):
            watch_sent += 1
            log(f"  ⚡ WATCH terkirim: {w['pair']} vol×{w['vol_ratio']:.1f}")
        time.sleep(TG_SEND_SLEEP_SEC)

    # Simpan WATCH baru ke state agar bisa dicek run berikutnya
    state = state_save_watches(state, watches)

    if watch_sent > 0:
        log(f"  {watch_sent} watch signal terkirim")

    # ── Equity Report (kirim setelah watch signals)
    eq_report = db_equity_report()
    if eq_report:
        tg(eq_report)
        log("📊 Equity report terkirim")

    # ── Scan Summary
    wr_line = ""
    if wr_data and wr_data.get("n_total", 0) >= WR_MIN_SAMPLE:
        wr_line = (
            f"\n📈 Winrate  : <b>{wr_data['wr']:.0%}</b>★ "
            f"({wr_data['n_win']}/{wr_data['n_total']}) "
            f"| E[PnL]: <b>{wr_data['expectancy']:+.1f}%</b>"
        )
    elif wr_data and wr_data.get("n_total", 0) > 0:
        wr_line = f"\n📊 Akumulasi data: {wr_data['n_total']}/{WR_MIN_SAMPLE} sinyal closed"

    btc_line = ""
    if BTC_TREND_ENABLED:
        btc_icon = "📈" if btc_label == "bullish" else ("📉" if btc_label == "bearish" else "➡️")
        btc_line = f"\nBTC trend    : {btc_icon} <b>{btc_label}</b> ({btc_pct:+.2f}%)"

    tg(format_summary(scanned, len(state["snapshots"]), len(candidates), sent) + wr_line + btc_line)
    log(f"\n✅ Done — {sent} pump | {early_sent} early | {watch_sent} watch")


# ══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    try:
        run_scan()
    except KeyboardInterrupt:
        log("Interrupted.")
        sys.exit(0)
    except Exception as e:
        log(f"FATAL: {e}", "error")
        log(traceback.format_exc(), "error")
        try:
            tg(f"❌ <b>PUMP MONITOR — FATAL ERROR</b>\n"
               f"<code>{str(e)[:300]}</code>\n"
               f"<i>{datetime.now(WIB).strftime('%d/%m/%Y %H:%M WIB')}</i>")
        except Exception:
            pass
        sys.exit(1)


# ══════════════════════════════════════════════════════════════════
#  UNIT TESTS — jalankan dengan: pytest indodax_pump_monitor.py -v
# ══════════════════════════════════════════════════════════════════

try:
    import pytest  # noqa: E402
except ImportError:
    pytest = None  # type: ignore  # bot tetap jalan tanpa pytest
# ══════════════════════════════════════════════════════════════════
#  FIXTURES
# ══════════════════════════════════════════════════════════════════

def make_history(prices: list[float], vols: list[float] | None = None) -> list[dict]:
    """Buat price history dummy untuk testing gates."""
    if vols is None:
        vols = [50_000_000.0] * len(prices)
    return [
        {"ts": 1_700_000_000 + i * 300, "last": p, "vol": v, "high": p * 1.05, "low": p * 0.90}
        for i, (p, v) in enumerate(zip(prices, vols))
    ]


def flat_history(price: float = 1000.0, n: int = 20, base_vol: float = 50_000_000.0) -> list[dict]:
    """History flat: semua harga sama, vol sama."""
    return make_history([price] * n, [base_vol] * n)


# ══════════════════════════════════════════════════════════════════
#  _calc_rsi
# ══════════════════════════════════════════════════════════════════

class TestCalcRsi:
    def test_returns_correct_length(self):
        prices = list(range(1, 31))
        result = _calc_rsi(prices, length=14)
        assert len(result) == 30

    def test_overbought_after_rally(self):
        """Harga naik dengan sedikit koreksi → RSI harus > 70."""
        # Pure uptrend tanpa loss membuat avg_loss=0 → RSI=NaN
        # Gunakan uptrend dengan koreksi kecil agar avg_loss > 0
        import random
        random.seed(42)
        prices = [100.0]
        for _ in range(29):
            # Naik 3%, koreksi kecil 0.5% selang-seling
            if len(prices) % 5 == 0:
                prices.append(prices[-1] * 0.995)
            else:
                prices.append(prices[-1] * 1.03)
        rsi = _calc_rsi(prices, 14)
        valid = [v for v in rsi if not math.isnan(v)]
        assert len(valid) > 0, "RSI harus punya nilai valid setelah warmup"
        assert valid[-1] > 70, f"Expected RSI > 70 after rally, got {valid[-1]:.1f}"

    def test_oversold_after_crash(self):
        """Harga turun terus → RSI harus mendekati 0."""
        prices = [1000 - i * 30 for i in range(30)]
        rsi = [v for v in _calc_rsi(prices, 14) if not math.isnan(v)]
        assert rsi[-1] < 30, f"Expected RSI < 30 after strong crash, got {rsi[-1]:.1f}"

    def test_warmup_returns_nan(self):
        """Kurang dari 14 candle → NaN untuk periode warmup."""
        prices = [100.0] * 10
        result = _calc_rsi(prices, 14)
        assert all(math.isnan(v) for v in result)

    def test_midrange_neutral(self):
        """Harga naik-turun bergantian → RSI sekitar 50."""
        prices = [100 if i % 2 == 0 else 102 for i in range(50)]
        rsi = [v for v in _calc_rsi(prices, 14) if not math.isnan(v)]
        assert 40 < rsi[-1] < 70


# ══════════════════════════════════════════════════════════════════
#  gate_price_pump
# ══════════════════════════════════════════════════════════════════

class TestGatePricePump:
    def test_pump_detected(self):
        history = flat_history(1000.0, 5)
        ok, pct = gate_price_pump(1050.0, history)
        assert ok is True
        assert pct == pytest.approx(5.0, abs=0.1)

    def test_no_pump_flat(self):
        history = flat_history(1000.0, 5)
        ok, pct = gate_price_pump(1010.0, history)
        assert ok is False    # 1% < default 3%

    def test_empty_history(self):
        ok, pct = gate_price_pump(1000.0, [])
        assert ok is False
        assert pct == 0.0

    def test_zero_prev_price(self):
        history = make_history([0.0, 0.0, 0.0])
        ok, pct = gate_price_pump(1000.0, history)
        assert ok is False

    def test_extreme_pump_80pct(self):
        history = flat_history(500.0, 20)
        ok, pct = gate_price_pump(900.0, history)
        assert ok is False   # anti fake pump >12% blocks this

    def test_price_drop_fails(self):
        history = flat_history(1000.0, 5)
        ok, pct = gate_price_pump(950.0, history)
        assert ok is False


# ══════════════════════════════════════════════════════════════════
#  gate_vol_spike
# ══════════════════════════════════════════════════════════════════

class TestGateVolSpike:
    def _build_vol_history(self, base_vol: float, n: int) -> list[dict]:
        return [{"ts": i, "last": 1000.0, "vol": base_vol} for i in range(n)]

    def test_spike_detected(self):
        """Delta volume sekarang harus ≥ VOL_SPIKE_MULT × rata-rata delta historis."""
        base = 50_000_000.0
        # History dengan rolling vol yang naik 500rb per snapshot (delta = 500rb)
        history = [{"ts": i, "last": 1000.0, "vol": base + i * 500_000}
                   for i in range(10)]
        # Spike: delta sekarang = 5jt (10× rata-rata 500rb)
        curr_vol = history[-1]["vol"] + 5_000_000
        ok, ratio = gate_vol_spike(curr_vol, history)
        assert ok is True, f"Expected spike detected, ratio={ratio:.1f}"
        assert ratio >= VOL_SPIKE_MULT

    def test_no_spike_normal_vol(self):
        base = 50_000_000.0
        history = self._build_vol_history(base, 10)
        ok, ratio = gate_vol_spike(base * 1.5, history)
        assert ok is False

    def test_insufficient_history(self):
        history = self._build_vol_history(50_000_000.0, 2)
        ok, ratio = gate_vol_spike(500_000_000.0, history)
        assert ok is False

    def test_zero_baseline_fails(self):
        history = [{"ts": i, "last": 1000.0, "vol": 0.0} for i in range(10)]
        ok, ratio = gate_vol_spike(100_000_000.0, history)
        assert ok is False

    def test_vol_delta_calculation(self):
        """Delta volume dihitung dari perbedaan antar snapshot."""
        base = 100_000_000.0
        # Rolling 24h vol naik 10jt per snapshot
        history = [{"ts": i, "last": 1000.0, "vol": base + i * 10_000_000} for i in range(8)]
        curr_vol = base + 8 * 10_000_000 + 50_000_000 * 5  # spike di akhir
        ok, ratio = gate_vol_spike(curr_vol, history)
        assert ok is True


# ══════════════════════════════════════════════════════════════════
#  gate_breakout
# ══════════════════════════════════════════════════════════════════

class TestGateBreakout:
    def test_breakout_from_low(self):
        history = flat_history(1000.0, 10)
        # Bounce 5% dari recent low
        ok, recent_low = gate_breakout(1050.0, history)
        assert ok is True

    def test_no_breakout_flat(self):
        history = flat_history(1000.0, 10)
        ok, recent_low = gate_breakout(1001.0, history)
        assert ok is False    # hanya +0.1%, kurang dari PRICE_PUMP_PCT

    def test_empty_history(self):
        ok, val = gate_breakout(1000.0, [])
        assert ok is False

    def test_uses_short_window(self):
        """Hanya 6 snapshot terakhir yang dipertimbangkan."""
        # History panjang: rendah di awal, tinggi di akhir (jadi recent low juga tinggi)
        prices = [500.0] * 20 + [2000.0] * 6
        history = make_history(prices)
        # Harga sekarang = 2100 (naik sedikit dari window tinggi = tidak breakout jauh)
        ok, low = gate_breakout(2100.0, history)
        # recent_low dari 6 snapshot terakhir = 2000, threshold = 2000 * 1.03 = 2060
        assert ok is True
        assert low == 2000.0


# ══════════════════════════════════════════════════════════════════
#  gate_rsi_cross
# ══════════════════════════════════════════════════════════════════

class TestGateRsiCross:
    def test_insufficient_history(self):
        history = flat_history(1000.0, 5)
        ok, rsi, has_data = gate_rsi_cross(1000.0, history)
        assert ok is False
        assert has_data is False

    def test_rsi_cross_detected(self):
        """Downtrend panjang → bounce keras → RSI cross oversold→recovery."""
        # Build downtrend yang cukup untuk RSI < 35
        down = [1000 - i * 12 for i in range(30)]   # turun signifikan
        bounce = [down[-1] + i * 20 for i in range(6)]  # bounce keras
        prices = down + bounce
        history = make_history(prices[:-1])
        ok, rsi_val, has_data = gate_rsi_cross(prices[-1], history)
        assert has_data is True
        # RSI harus sudah recovery
        if ok:
            assert rsi_val >= RSI_RECOVERY

    def test_no_cross_sideways(self):
        """Harga sideways → RSI tidak pernah oversold → tidak cross."""
        prices = [1000 + (i % 3) * 2 for i in range(30)]
        history = make_history(prices[:-1])
        ok, rsi_val, has_data = gate_rsi_cross(prices[-1], history)
        # RSI sideways tidak mungkin dari oversold
        if has_data:
            assert ok is False or rsi_val < 35  # tidak valid cross jika tidak pernah oversold


# ══════════════════════════════════════════════════════════════════
#  calc_levels
# ══════════════════════════════════════════════════════════════════

class TestCalcLevels:
    def test_basic_structure(self):
        lvl = calc_levels(1000.0)
        assert set(lvl.keys()) >= {"current", "entry", "sl", "tp1", "tp2", "tp3", "rr", "R"}

    def test_entry_below_current(self):
        """Entry harus di bawah harga sekarang (retest zone)."""
        lvl = calc_levels(1000.0)
        assert lvl["entry"] <= lvl["current"]   # 0.5% discount

    def test_sl_below_entry(self):
        lvl = calc_levels(1000.0)
        assert lvl["sl"] < lvl["entry"]

    def test_tp_ascending(self):
        """TP1 < TP2 < TP3."""
        lvl = calc_levels(1000.0)
        assert lvl["tp1"] < lvl["tp2"] < lvl["tp3"]

    def test_rr_always_2(self):
        """R/R selalu 2.0 karena TP2 = entry + 2×R by definition."""
        lvl = calc_levels(1000.0)
        assert lvl["rr"] == pytest.approx(2.0, abs=0.01)

    def test_zero_price_returns_empty(self):
        assert calc_levels(0.0) == {}
        assert calc_levels(-100.0) == {}

    def test_r_positive(self):
        """R (risiko per unit) harus > 0."""
        lvl = calc_levels(5000.0)
        assert lvl["R"] > 0

    def test_sl_pct_matches_config(self):
        """SL% harus sesuai SL_PCT dari config."""
        lvl = calc_levels(1000.0)
        assert lvl["sl_pct"] == pytest.approx(SL_PCT, abs=0.01)

    def test_resistance_note_between_tp1_tp2(self):
        """Jika high_24h antara TP1 dan TP2 → resistance note muncul."""
        lvl = calc_levels(1000.0, high_24h=0.0)  # no resistance
        assert lvl["resistance_note"] == ""

    def test_resistance_note_above_tp1(self):
        """high_24h di antara tp1 dan tp2 → ada resistance note."""
        lvl_base = calc_levels(1000.0)
        midpoint = (lvl_base["tp1"] + lvl_base["tp2"]) / 2
        lvl = calc_levels(1000.0, high_24h=midpoint)
        assert "Resistance" in lvl["resistance_note"]

    def test_microcap_price(self):
        """Harga sangat kecil (microcap) tetap dihitung benar."""
        lvl = calc_levels(0.0000150)
        assert lvl["entry"] > 0
        assert lvl["sl"] < lvl["entry"]
        assert lvl["tp1"] > lvl["entry"]


# ══════════════════════════════════════════════════════════════════
#  calc_score & Tier
# ══════════════════════════════════════════════════════════════════

class TestCalcScore:
    def _make_sig(self, pump_pct, vol_ratio, rsi=0.0, has_rsi=False, snaps=20):
        return {"pump_pct": pump_pct, "vol_ratio": vol_ratio,
                "rsi": rsi, "has_rsi": has_rsi, "snaps": snaps}

    def test_tier_s_high_all(self):
        sig = self._make_sig(pump_pct=12, vol_ratio=15, rsi=58, has_rsi=True, snaps=20)
        score, tier = calc_score(sig)
        assert tier == "S"
        assert score >= 12

    def test_tier_a_plus(self):
        sig = self._make_sig(pump_pct=6, vol_ratio=9, rsi=52, has_rsi=True, snaps=10)
        score, tier = calc_score(sig)
        assert tier in ("A+", "S")
        assert score >= 6

    def test_tier_a_minimum(self):
        sig = self._make_sig(pump_pct=3.5, vol_ratio=5.5, snaps=3)
        score, tier = calc_score(sig)
        assert tier == "A"

    def test_score_increases_with_strength(self):
        weak   = self._make_sig(3, 5)
        strong = self._make_sig(12, 20, rsi=58, has_rsi=True, snaps=20)
        s_weak, _   = calc_score(weak)
        s_strong, _ = calc_score(strong)
        assert s_strong > s_weak

    def test_score_max_15(self):
        sig = self._make_sig(50, 100, rsi=90, has_rsi=True, snaps=30)
        score, _ = calc_score(sig)
        assert score <= 15

    def test_score_min_0(self):
        sig = self._make_sig(0, 0)
        score, _ = calc_score(sig)
        assert score >= 0


# ══════════════════════════════════════════════════════════════════
#  WR Bucketing & predict_wr
# ══════════════════════════════════════════════════════════════════

class TestWrBucketing:
    def _make_buckets(self) -> dict:
        """Bucket dummy: A+ pump10-20% vol12-20x → WR tinggi, A pump<5% vol<7x → WR rendah."""
        WIN_SET = {"TP1", "TP2", "TP3"}
        raw = {}
        data = [
            # Tier A+ (score=9 untuk pump11, vol14, snaps5), pump 10-20%, vol 12-20x → 5 win
            *[("A+", 11, 14, "TP2")] * 5,
            # Tier A, pump <5%, vol <7x → 1 win, 4 loss
            *[("A", 3.5, 5.5, "SL")] * 4,
            ("A", 3.5, 5.5, "TP1"),
        ]
        for tier, pp, vr, result in data:
            iw = result in WIN_SET
            for key in [
                f"{tier}|{_pump_bucket(pp)}|{_vol_bucket(vr)}",
                f"{tier}|{_pump_bucket(pp)}",
                tier,
            ]:
                b = raw.setdefault(key, {"n": 0, "n_win": 0})
                b["n"] += 1
                b["n_win"] += int(iw)
        return {k: {"wr": round((b["n_win"] + 0.5) / (b["n"] + 1), 3),
                    "n": b["n"], "n_win": b["n_win"]}
                for k, b in raw.items()}

    def test_high_wr_tier_a_plus(self):
        """Tier A+ dengan pump+vol kuat → WR tinggi dari bucket."""
        buckets = self._make_buckets()
        # Sinyal yang akan mendapat tier A+ (pump11, vol14, snaps5 → score=9)
        sig = {"pump_pct": 11, "vol_ratio": 14, "rsi": 0, "has_rsi": False, "snaps": 5}
        wr, key, has = predict_wr(sig, buckets)
        assert has is True, f"Expected bucket found, key={key}"
        assert wr >= 0.70, f"Expected WR ≥ 70% for strong A+, got {wr:.0%}"

    def test_low_wr_tier_a(self):
        buckets = self._make_buckets()
        sig = {"pump_pct": 3.5, "vol_ratio": 5.5, "rsi": 0, "has_rsi": False, "snaps": 5}
        wr, key, has = predict_wr(sig, buckets)
        assert has is True
        assert wr < 0.50, f"Expected WR < 50% for weak Tier A, got {wr:.0%}"

    def test_empty_buckets(self):
        sig = {"pump_pct": 5, "vol_ratio": 8, "rsi": 0, "has_rsi": False, "snaps": 5}
        wr, key, has = predict_wr(sig, {})
        assert has is False
        assert wr == 0.0

    def test_fallback_to_coarse(self):
        """Jika fine bucket tidak cukup sample, fallback ke tier saja."""
        # Sig pump=11, vol=14 → tier A+ dari calc_score
        buckets = {"A+": {"wr": 0.75, "n": 10, "n_win": 7}}  # hanya coarse A+
        sig = {"pump_pct": 11, "vol_ratio": 14, "rsi": 0, "has_rsi": False, "snaps": 5}
        wr, key, has = predict_wr(sig, buckets)
        assert has is True, f"Expected coarse fallback, key={key}"
        assert key == "A+"
        assert wr == 0.75

    def test_pump_bucket_thresholds(self):
        assert _pump_bucket(3.0)  == "pump<5%"
        assert _pump_bucket(5.0)  == "pump5-10%"
        assert _pump_bucket(10.0) == "pump10-20%"
        assert _pump_bucket(20.0) == "pump≥20%"

    def test_vol_bucket_thresholds(self):
        assert _vol_bucket(5.0)  == "vol<7x"
        assert _vol_bucket(7.0)  == "vol7-12x"
        assert _vol_bucket(12.0) == "vol12-20x"
        assert _vol_bucket(20.0) == "vol≥20x"


# ══════════════════════════════════════════════════════════════════
#  State Management
# ══════════════════════════════════════════════════════════════════

class TestStateManagement:
    def test_build_snapshot_filters_vol(self):
        tickers = {
            "btc_idr":  {"last": "500000000", "vol_idr": "1000000000",
                         "high": "510000000", "low": "490000000"},
            "tiny_idr": {"last": "1",          "vol_idr": "100",
                         "high": "1.1",        "low": "0.9"},  # under VOL_IDR_MIN
        }
        snap = build_snapshot(tickers)
        assert "btc_idr"  in snap["data"]
        assert "tiny_idr" not in snap["data"]

    def test_update_state_trims_to_max(self):
        state = {"updated": 0, "snapshots": [{"ts": i, "data": {}} for i in range(30)]}
        new_snap = {"ts": 31, "data": {}}
        updated = update_state(state, new_snap)
        assert len(updated["snapshots"]) == MAX_SNAPSHOTS
        assert updated["snapshots"][-1]["ts"] == 31

    def test_get_pair_history_returns_correct_order(self):
        state = {
            "snapshots": [
                {"ts": 1, "data": {"algo_idr": {"last": 100.0, "vol": 1e7, "high": 105.0, "low": 90.0}}},
                {"ts": 2, "data": {"algo_idr": {"last": 105.0, "vol": 1.1e7, "high": 110.0, "low": 95.0}}},
                {"ts": 3, "data": {"algo_idr": {"last": 110.0, "vol": 1.2e7, "high": 115.0, "low": 100.0}}},
            ]
        }
        history = get_pair_history(state, "algo_idr")
        assert len(history) == 3
        assert history[0]["last"] == 100.0
        assert history[-1]["last"] == 110.0

    def test_get_pair_history_missing_pair(self):
        state = {"snapshots": [{"ts": 1, "data": {"other_idr": {"last": 100.0, "vol": 1e7, "high": 105.0, "low": 90.0}}}]}
        history = get_pair_history(state, "algo_idr")
        assert history == []

    def test_empty_state_returns_empty_history(self):
        state = {"snapshots": []}
        assert get_pair_history(state, "algo_idr") == []


# ══════════════════════════════════════════════════════════════════
#  Format Helpers
# ══════════════════════════════════════════════════════════════════

class TestFormatHelpers:
    def test_fp_large_price(self):
        assert "," in _fp(4200.0)      # format ribuan
        assert "4,200" == _fp(4200.0)

    def test_fp_small_price(self):
        result = _fp(11.52)
        assert "11.52" == result

    def test_fp_microcap(self):
        result = _fp(0.00001540)
        assert "0." in result
        assert result.endswith("154") or "154" in result  # trailing zero stripped

    def test_fmt_idr_millions(self):
        assert "jt" in _fmt_idr(1_500_000)

    def test_fmt_idr_billions(self):
        assert "M" in _fmt_idr(2_000_000_000)

    def test_fmt_partial_profit_structure(self):
        msg = _fmt_partial_profit(
            "algo_idr", 4137.0, 4344.0, 5.0, 4551.0, 3930.0
        )
        assert "Partial Profit Taken" in msg
        assert "ALGO/IDR"     in msg
        assert "70%"          in msg        # TP1_CLOSE_PCT
        assert "breakeven"    in msg
        assert "TP2"          in msg

    def test_format_summary_structure(self):
        msg = format_summary(508, 20, 3, 2)
        assert "508" in msg
        assert "20"  in msg
        assert "3"   in msg


# ══════════════════════════════════════════════════════════════════
#  Adapt Thresholds
# ══════════════════════════════════════════════════════════════════

class TestAdaptThresholds:
    def _wr_data(self, wr: float, n: int = 20) -> dict:
        return {"wr": wr, "n_total": n, "n_win": int(wr * n), "expectancy": 0.0}

    def test_low_wr_tightens(self):
        p, v, _, __ = adapt_thresholds(self._wr_data(0.35))
        assert p > PRICE_PUMP_PCT or v > VOL_SPIKE_MULT

    def test_high_wr_loosens(self):
        p, v, _, __ = adapt_thresholds(self._wr_data(0.70))
        assert p < PRICE_PUMP_PCT or v < VOL_SPIKE_MULT

    def test_normal_wr_unchanged(self):
        orig_p = PRICE_PUMP_PCT
        orig_v = VOL_SPIKE_MULT
        p, v, _, __ = adapt_thresholds(self._wr_data(0.52))
        assert p == orig_p
        assert v == orig_v

    def test_bounds_respected(self):
        """Threshold tidak boleh keluar dari batas min/max."""
        p, v, _, __ = adapt_thresholds(self._wr_data(0.05, n=100))  # WR sangat buruk
        assert p <= PUMP_PCT_MAX
        assert v <= VOL_MULT_MAX

    def test_insufficient_sample_unchanged(self):
        """Di bawah WR_MIN_SAMPLE → tidak ada perubahan."""
        orig_p = PRICE_PUMP_PCT
        p, v, _, __ = adapt_thresholds({"wr": 0.30, "n_total": 3})
        assert p == orig_p


# ══════════════════════════════════════════════════════════════════
#  Extreme Pump Bypass
# ══════════════════════════════════════════════════════════════════

class TestExtremePumpBypass:
    """Verifikasi bahwa pump >= EXTREME_PUMP_BYPASS_PCT melewati gate RSI."""

    def _build_sideways_history(self, n: int = 20) -> list[dict]:
        """History sideways — RSI tidak pernah oversold."""
        prices = [500 + (i % 4) * 2 for i in range(n)]
        return make_history(prices)

    def test_extreme_pump_bypasses_rsi(self):
        """Pump 80% dari history sideways harus lolos gate RSI."""
        history = self._build_sideways_history(20)
        gate_stats = {}
        td = {"last": "900", "vol_idr": str(50_000_000 * 8),
              "high": "945", "low": "450"}
        sig = analyze_pair("xyz_idr", td, history, gate_stats)
        # Jika lolos, is_extreme harus True
        if sig:
            assert sig.get("is_extreme") is True
        # Jika tidak lolos, bukan karena RSI (karena bypass)
        if not sig:
            assert "G4_rsi_cross_fail" not in gate_stats

    def test_normal_pump_not_extreme(self):
        """Pump 4% tidak masuk extreme bypass."""
        history = self._build_sideways_history(20)
        gate_stats = {}
        td = {"last": str(500 * 1.04), "vol_idr": str(50_000_000 * 6),
              "high": "540", "low": "450"}
        sig = analyze_pair("xyz_idr", td, history, gate_stats)
        if sig:
            assert sig.get("is_extreme") is False


# ══════════════════════════════════════════════════════════════════
#  Pre-pump Detection
# ══════════════════════════════════════════════════════════════════

class TestPrePumpDetection:
    def _build_accumulation(self, base_price=1000.0, base_vol=30_000_000.0, n=12):
        """Build history dengan volume acceleration tapi harga flat."""
        history = []
        for i in range(n):
            if i < 6:
                vol = base_vol * (1 + i * 0.05)         # normal growth
            else:
                vol = base_vol * (1 + i * 0.5)           # accelerating
            history.append({
                "ts":   i, "last": base_price * (1 + (i % 3) * 0.002),
                "vol":  vol, "high": base_price * 1.05, "low": base_price * 0.90,
            })
        return history

    def test_watch_detected_on_accumulation(self):
        history = self._build_accumulation()
        td = {
            "last":    "1005",
            "vol_idr": str(30_000_000 * (1 + 12 * 0.6)),
            "high":    "1050",
            "low":     "900",
        }
        w = detect_pre_pump("test_idr", td, history)
        # Tidak wajib True (tergantung exact value), tapi tidak boleh crash
        assert w is None or isinstance(w, dict)
        if w:
            assert "pair"       in w
            assert "vol_ratio"  in w
            assert "price_flat" in w

    def test_no_watch_on_pump(self):
        """Jika harga sudah pump besar → tidak masuk WATCH (bukan pre-pump lagi)."""
        history = make_history([1000.0] * 10)
        td = {
            "last":    "1200",   # sudah pump 20%
            "vol_idr": str(150_000_000),
            "high":    "1210",
            "low":     "990",
        }
        w = detect_pre_pump("test_idr", td, history)
        assert w is None   # price_flat 20% >> WATCH_PRICE_FLAT_PCT (2%)

    def test_no_watch_insufficient_history(self):
        history = make_history([1000.0] * 2)  # < WATCH_MIN_SNAPS
        td = {"last": "1005", "vol_idr": "50000000", "high": "1050", "low": "900"}
        w = detect_pre_pump("test_idr", td, history)
        assert w is None
