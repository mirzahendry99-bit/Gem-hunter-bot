"""
╔══════════════════════════════════════════════════════════════════╗
║          INDODAX PUMP SIGNAL MONITOR  v2.1  (STATEFUL + DB)      ║
║                                                                  ║
║  Target    : Semua pair IDR microcap di Indodax                  ║
║  Stack     : Indodax Public API + pandas + Supabase + Telegram   ║
║  Mode      : Stateful + Outcome Tracking + Adaptive Threshold    ║
║                                                                  ║
║  Signal Logic v2 (SEMUA harus terpenuhi):                        ║
║  1. Price Pump    — harga naik ≥ PRICE_PUMP_PCT% vs snapshot lalu║
║  2. Volume Spike  — Δvol_idr ≥ VOL_SPIKE_MULT × baseline Δvol   ║
║  3. Breakout      — bounce ≥ 3% dari recent low (6 snap window)  ║
║  4. RSI Cross     — RSI(14) dari ≤35 ke ≥50 (dari price series)  ║
║                                                                  ║
║  Learning:                                                       ║
║  - Outcome tracker: setiap run cek TP1/TP2/TP3/SL open signals  ║
║  - Bayesian winrate dari tabel indodax_signals di Supabase       ║
║  - Adaptive threshold: ketat/longgar otomatis berdasarkan WR     ║
║                                                                  ║
║  ENV VARS (opsional):                                            ║
║    SUPABASE_URL / SUPABASE_KEY — jika ada, aktifkan DB tracking  ║
║    PRICE_PUMP_PCT  float default=3.0                             ║
║    VOL_SPIKE_MULT  float default=5.0                             ║
║    RSI_OVERSOLD    float default=35.0                            ║
║    RSI_RECOVERY    float default=50.0                            ║
║    SL_PCT/TP1/TP2/TP3_PCT  float 5/5/10/20                       ║
║    SIGNAL_EXPIRE_HOURS  int  default=48                          ║
║    WR_MIN_SAMPLE   int  default=15  (min sinyal utk adaptive)    ║
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

TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID: str   = os.environ["TELEGRAM_CHAT_ID"]
_TG_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Signal thresholds (bisa di-override oleh adaptive_thresholds())
PRICE_PUMP_PCT = float(os.environ.get("PRICE_PUMP_PCT", "3.0"))
VOL_SPIKE_MULT = float(os.environ.get("VOL_SPIKE_MULT", "5.0"))
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
ENTRY_DISCOUNT_PCT = float(os.environ.get("ENTRY_DISCOUNT_PCT", "1.5"))

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
PUMP_PCT_MIN, PUMP_PCT_MAX = 2.0, 8.0
VOL_MULT_MIN,  VOL_MULT_MAX  = 3.0, 12.0
# Bayesian prior (Jeffreys)
_BAYES_A, _BAYES_B = 0.5, 0.5

# Runtime
REQUEST_TIMEOUT_SEC = int(os.environ.get("REQUEST_TIMEOUT",     "10"))
TG_SEND_SLEEP_SEC   = float(os.environ.get("TG_SEND_SLEEP_SEC", "1.0"))
MAX_SIGNALS_PER_RUN = int(os.environ.get("MAX_SIGNALS_PER_RUN", "5"))

# Indodax API
INDODAX_TICKER_ALL = "https://indodax.com/api/ticker_all"

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

def gate_price_pump(curr: float, history: list[dict]) -> tuple[bool, float]:
    """Gate 1: harga sekarang vs snapshot terakhir ≥ PRICE_PUMP_PCT%"""
    if not history:
        return False, 0.0
    prev = history[-1]["last"]
    if prev <= 0:
        return False, 0.0
    pct = (curr - prev) / prev * 100
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
    Gate 3: harga sekarang ≥ min harga 6 snapshot terakhir × (1 + PRICE_PUMP_PCT%).
    Semantik: konfirmasi bounce signifikan dari recent low dalam window 30 menit.
    Ini lebih relevan untuk pump detection dari bottom daripada breakout all-time high.
    """
    if not history:
        return False, 0.0
    short_window = history[-6:]
    recent_low   = min(h["last"] for h in short_window)
    if recent_low <= 0:
        return False, 0.0
    threshold = recent_low * (1 + PRICE_PUMP_PCT / 100)
    return curr >= threshold, round(recent_low, 8)


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


# ══════════════════════════════════════════════════════════════════
#  RISK LEVELS
# ══════════════════════════════════════════════════════════════════

def calc_levels(current_price: float) -> dict:
    """
    Hitung level trading dari harga sekarang.

    Entry zone = current_price × (1 - ENTRY_DISCOUNT_PCT/100)
    → sedikit di bawah harga pump untuk hindari chasing.
    SL/TP dihitung dari entry zone (bukan current_price).
    """
    if current_price <= 0:
        return {}
    entry = current_price * (1 - ENTRY_DISCOUNT_PCT / 100)
    sl    = entry * (1 - SL_PCT  / 100)
    tp1   = entry * (1 + TP1_PCT / 100)
    tp2   = entry * (1 + TP2_PCT / 100)
    tp3   = entry * (1 + TP3_PCT / 100)
    rr    = round((tp2 - entry) / (entry - sl), 1) if entry > sl else 0.0
    return {
        "current": current_price,
        "entry":   round(entry, 8),
        "sl":      round(sl,    8),
        "tp1":     round(tp1,   8),
        "tp2":     round(tp2,   8),
        "tp3":     round(tp3,   8),
        "rr":      rr,
    }


# ══════════════════════════════════════════════════════════════════
#  PAIR ANALYZER
# ══════════════════════════════════════════════════════════════════

def analyze_pair(
    pair:        str,
    ticker_data: dict,
    history:     list[dict],
    gate_stats:  dict,
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
            _reject(f"STATE_need_more_snaps"); return None

        # Gate 1: Price Pump
        pump_ok, pump_pct = gate_price_pump(curr_price, history)
        if not pump_ok:
            _reject("G1_price_pump_fail"); return None

        # Gate 2: Volume Spike
        vol_ok, vol_ratio = gate_vol_spike(vol_idr, history)
        if not vol_ok:
            _reject("G2_vol_spike_fail"); return None

        # Gate 3: Breakout
        break_ok, prev_max = gate_breakout(curr_price, history)
        if not break_ok:
            _reject("G3_breakout_fail"); return None

        # Gate 4: RSI (dilewati jika history belum cukup)
        rsi_ok, curr_rsi, has_rsi = gate_rsi_cross(curr_price, history)
        if has_rsi and not rsi_ok:
            _reject("G4_rsi_cross_fail"); return None

        coin = pair.replace("_idr", "").upper()
        return {
            "pair":      pair,
            "coin":      coin,
            "price":     curr_price,
            "vol_idr":   vol_idr,
            "vol_ratio": vol_ratio,
            "pump_pct":  pump_pct,
            "high_24h":  high_24h,
            "low_24h":   low_24h,
            "prev_max":  prev_max,
            "rsi":       curr_rsi,
            "has_rsi":   has_rsi,
            "snaps":     len(history),
            "ts":        datetime.now(WIB),
        }

    except Exception as e:
        log(f"analyze_pair({pair}): {e}", "warn")
        _reject("EXCEPTION")
        return None


# ══════════════════════════════════════════════════════════════════
#  DATABASE — OUTCOME TRACKING & ADAPTIVE LEARNING
# ══════════════════════════════════════════════════════════════════

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
      result           text,
      closed_at        timestamptz,
      pnl_pct          numeric,
      threshold_pump   numeric,
      threshold_vol    numeric
    );
    """
    sb = _get_sb()
    if not sb or not lvl:
        return None
    try:
        row = {
            "pair":           sig["pair"],
            "entry":          lvl["entry"],    # entry zone (discounted), bukan harga pump
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


def db_evaluate_outcomes() -> dict:
    """
    Cek semua open signals (result IS NULL) dan update outcome
    berdasarkan harga ticker Indodax sekarang.

    Logic per row:
      price >= tp3 → TP3 (win terbesar)
      price >= tp2 → TP2
      price >= tp1 → TP1
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
            .select("id, pair, entry, sl, tp1, tp2, tp3, sent_at")
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

    # Fetch semua ticker sekarang (1 call untuk semua pair)
    try:
        resp     = requests.get(INDODAX_TICKER_ALL, timeout=REQUEST_TIMEOUT_SEC)
        tickers  = resp.json().get("tickers", {})
    except Exception:
        tickers = {}

    now_utc = datetime.now(timezone.utc)

    for row in rows:
        stats["evaluated"] += 1
        try:
            rec_id = row["id"]
            pair   = row["pair"]
            entry  = float(row["entry"])
            sl     = float(row["sl"])
            tp1    = float(row["tp1"])
            tp2    = float(row["tp2"])
            tp3    = float(row["tp3"])

            # Cek expired
            sent_at = datetime.fromisoformat(row["sent_at"].replace("Z", "+00:00"))
            age_h   = (now_utc - sent_at).total_seconds() / 3600
            if age_h > SIGNAL_EXPIRE_HOURS:
                result  = "EXPIRED"
                pnl_pct = 0.0
            else:
                td      = tickers.get(pair, {})
                price   = float(td.get("last", 0) or 0)
                if price <= 0:
                    continue   # ticker tidak ditemukan, coba lagi run berikutnya

                if price >= tp3:
                    result  = "TP3"
                    pnl_pct = round((tp3 - entry) / entry * 100, 2)
                elif price >= tp2:
                    result  = "TP2"
                    pnl_pct = round((tp2 - entry) / entry * 100, 2)
                elif price >= tp1:
                    result  = "TP1"
                    pnl_pct = round((tp1 - entry) / entry * 100, 2)
                elif price <= sl:
                    result  = "SL"
                    pnl_pct = round((sl - entry) / entry * 100, 2)
                else:
                    continue   # masih open, tidak ada outcome

            # Update DB
            sb.table(DB_TABLE).update({
                "result":    result,
                "pnl_pct":   pnl_pct,
                "closed_at": now_utc.isoformat(),
            }).eq("id", rec_id).execute()

            stats[result.lower()] = stats.get(result.lower(), 0) + 1
            log(f"  {pair} → {result} ({pnl_pct:+.1f}%)")

        except Exception as e:
            log(f"  outcome [{row.get('pair')}]: {e}", "warn")

    return stats


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
    n_loss   = n_total - n_win

    # Bayesian posterior (Jeffreys' prior)
    wr_bayes = (n_win + _BAYES_A) / (n_total + _BAYES_A + _BAYES_B)
    wr_freq  = n_win / n_total

    # Expectancy: avg pnl_pct semua sinyal closed
    pnl_vals    = [float(r["pnl_pct"]) for r in rows if r.get("pnl_pct") is not None]
    expectancy  = round(sum(pnl_vals) / len(pnl_vals), 2) if pnl_vals else 0.0

    return {
        "wr":          round(wr_bayes, 3),
        "wr_freq":     round(wr_freq,  3),
        "n_total":     n_total,
        "n_win":       n_win,
        "expectancy":  expectancy,
    }


def adapt_thresholds(wr_data: dict) -> tuple[float, float]:
    """
    Sesuaikan PRICE_PUMP_PCT dan VOL_SPIKE_MULT berdasarkan winrate historis.

    Logic:
      WR < 40%  → perketat threshold +15% (terlalu banyak false positive)
      WR 40-55% → pertahankan threshold sekarang
      WR > 65%  → longgarkan threshold -10% (bisa catch lebih banyak sinyal)

    Bounds: PUMP [2.0, 8.0] | VOL [3.0, 12.0]
    Return (new_pump_pct, new_vol_mult)
    """
    if not wr_data or wr_data.get("n_total", 0) < WR_MIN_SAMPLE:
        return PRICE_PUMP_PCT, VOL_SPIKE_MULT

    wr = wr_data["wr"]

    if wr < 0.40:
        factor = 1.15   # perketat
        action = f"WR={wr:.0%} < 40% → PERKETAT threshold ×{factor}"
    elif wr > 0.65:
        factor = 0.90   # longgarkan
        action = f"WR={wr:.0%} > 65% → LONGGARKAN threshold ×{factor}"
    else:
        log(f"📊 Adaptive: WR={wr:.0%} dalam range normal — threshold tidak berubah")
        return PRICE_PUMP_PCT, VOL_SPIKE_MULT

    new_pump = round(max(PUMP_PCT_MIN, min(PUMP_PCT_MAX, PRICE_PUMP_PCT * factor)), 2)
    new_vol  = round(max(VOL_MULT_MIN, min(VOL_MULT_MAX, VOL_SPIKE_MULT * factor)), 2)
    log(f"📊 Adaptive: {action}")
    log(f"   pump: {PRICE_PUMP_PCT}% → {new_pump}% | vol: {VOL_SPIKE_MULT}× → {new_vol}×")
    return new_pump, new_vol


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
    """
    Format sinyal pump ke Telegram — layout mengikuti style Gate.io Signal Bot:
    header → pair + valid until → entry note → level table → metadata → footer.
    """
    ts           = sig["ts"]
    pair_display = sig["pair"].replace("_", "/").upper()
    lvl          = calc_levels(sig["price"])
    if not lvl:
        return f"🚨 <b>PUMP — {sig['coin']}</b>\n{pair_display}\n⚠️ Level tidak bisa dihitung."

    now         = ts
    valid_until = (now + timedelta(hours=SIGNAL_EXPIRE_HOURS)).strftime("%H:%M WIB")
    now_str     = now.strftime("%H:%M")

    # ── Strength label berdasarkan pump% + vol ratio
    pump  = sig["pump_pct"]
    vol_r = sig.get("vol_ratio", 0)
    if pump >= 8 and vol_r >= 8:
        strength     = "💎 STRONG"
        strength_bar = "█████"
    elif pump >= 5 and vol_r >= 5:
        strength     = "🏆 SOLID"
        strength_bar = "████░"
    else:
        strength     = "🥇 VALID"
        strength_bar = "███░░"

    # ── Entry note: apakah harga sudah jauh dari entry?
    # Pump baru saja terjadi — entry masih relevan jika dalam ±1%
    entry_note = ""
    if pump > 5.0:
        entry_note = (
            f"\n⚠️ <i>Pump sudah {pump:.1f}% — masuk hanya jika ada retest/koreksi "
            f"ke zona entry. Jangan kejar harga!</i>"
        )

    # ── RSI line
    rsi_line = (
        f"RSI(14)    : <b>{sig['rsi']:.1f}</b>\n"
        if sig.get("has_rsi") and sig["rsi"] > 0
        else f"RSI(14)    : <i>akumulasi ({sig['snaps']}/{MIN_SNAPS_RSI} snap)</i>\n"
    )

    # ── Hist WR dari Supabase (jika tersedia)
    if wr_data and wr_data.get("n_total", 0) >= WR_MIN_SAMPLE:
        hist_wr = (
            f"Hist WR    : <b>{wr_data['wr']:.0%}★</b> "
            f"({wr_data['n_win']}/{wr_data['n_total']}) "
            f"E[PnL]: <b>{wr_data['expectancy']:+.1f}%</b>\n"
        )
    else:
        n = wr_data.get("n_total", 0) if wr_data else 0
        hist_wr = f"Hist WR    : <i>akumulasi data ({n}/{WR_MIN_SAMPLE})</i>\n"

    # ── Adaptive threshold label
    thresh_label = f"Threshold  : pump≥{PRICE_PUMP_PCT:.1f}% | vol≥{VOL_SPIKE_MULT:.1f}×\n"

    # ── Support line
    low_ref  = sig.get("low_24h", 0)
    low_line = f"└ Support : <code>{_fp(low_ref)}</code>  <i>(low 24h)</i>\n" if low_ref > 0 else ""

    msg = (
        f"🚨 <b>{strength} — PUMP {sig['coin']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pair       : <b>{pair_display}</b>\n"
        f"⏰ Valid    : {now_str} → {valid_until}\n"
        f"Strength   : {strength_bar}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Harga skrg : <b>{_fp(lvl['current'])}</b> IDR\n"
        f"Entry Zone : <b>{_fp(lvl['entry'])}</b> IDR  "
        f"<i>(-{ENTRY_DISCOUNT_PCT:.1f}% retest)</i>{entry_note}\n"
        f"TP1        : <b>{_fp(lvl['tp1'])}</b>  <i>(+{TP1_PCT:.0f}%)</i>\n"
        f"TP2        : <b>{_fp(lvl['tp2'])}</b>  <i>(+{TP2_PCT:.0f}%)</i>\n"
        f"TP3        : <b>{_fp(lvl['tp3'])}</b>  <i>(+{TP3_PCT:.0f}%)</i>\n"
        f"{low_line}"
        f"SL         : <b>{_fp(lvl['sl'])}</b>  <i>(-{SL_PCT:.0f}%)</i>\n"
        f"R/R        : <b>1:{lvl['rr']}</b> (vs TP2)\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Pump       : <b>+{sig['pump_pct']:.2f}%</b> vs snapshot lalu\n"
        f"Vol Spike  : <b>{sig['vol_ratio']:.1f}×</b> baseline\n"
        f"{rsi_line}"
        f"Vol 24h    : {_fmt_idr(sig['vol_idr'])}\n"
        f"Snaps      : {sig['snaps']} snapshot window\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{hist_wr}"
        f"{thresh_label}"
        f"<i>⚠️ Pasang SL wajib. Bukan saran investasi.</i>"
    )
    return msg


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
    log("=" * 64)
    log("🔍 INDODAX PUMP SIGNAL MONITOR v2.1 (STATEFUL + LEARNING)")
    log(f"  Pump≥{PRICE_PUMP_PCT}% | Vol≥{VOL_SPIKE_MULT}× | RSI {RSI_OVERSOLD:.0f}→{RSI_RECOVERY:.0f}")
    log(f"  State: {STATE_FILE} | Max snaps: {MAX_SNAPSHOTS} | Min snaps: {MIN_SNAPS_SIGNAL}")
    log("=" * 64)

    if not validate_telegram():
        log("Scan dibatalkan — perbaiki konfigurasi Telegram.", "error")
        sys.exit(1)

    # Load state
    state  = load_state()
    n_snaps = len(state.get("snapshots", []))
    if n_snaps == 0:
        log("⚠️  Run pertama — hanya kumpulkan data, belum ada sinyal.", "warn")
    elif n_snaps < MIN_SNAPS_SIGNAL:
        log(f"⚠️  Masih akumulasi data: {n_snaps}/{MIN_SNAPS_SIGNAL} snapshot.", "warn")

    # ── Step 1b: Outcome tracking — update sinyal open dari run sebelumnya
    outcome_stats = db_evaluate_outcomes()
    if outcome_stats.get("evaluated", 0) > 0:
        log(f"📋 Outcome: {outcome_stats}")

    # ── Step 1c: Load winrate & adaptive threshold
    wr_data = db_load_winrate()
    PRICE_PUMP_PCT, VOL_SPIKE_MULT = adapt_thresholds(wr_data)

    # Fetch tickers
    log("Fetching ticker Indodax...")
    tickers = fetch_all_tickers()
    if not tickers:
        log("Gagal fetch tickers — abort.", "error")
        return
    log(f"Total pair IDR: {len(tickers)}")

    # Scan
    scanned    = 0
    candidates: list[dict] = []
    gate_stats: dict[str, int] = {}

    for pair, ticker_data in tickers.items():
        scanned += 1
        history = get_pair_history(state, pair)
        sig     = analyze_pair(pair, ticker_data, history, gate_stats)
        if sig:
            candidates.append(sig)
            log(
                f"  ✔ {pair:20s} pump={sig['pump_pct']:+.1f}% "
                f"vol×{sig['vol_ratio']:.1f} rsi={sig['rsi']:.1f} "
                f"snaps={sig['snaps']}",
            )

    log(f"\nScan: {scanned} pair | {n_snaps} snap lama | {len(candidates)} sinyal")

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
    candidates.sort(key=lambda x: (-x["pump_pct"], -x["vol_ratio"]))
    sent = 0
    for sig in candidates[:MAX_SIGNALS_PER_RUN]:
        lvl = calc_levels(sig["price"])
        if tg(format_signal(sig, wr_data)):
            sent += 1
            db_save_signal(sig, lvl)
            log(f"  📤 {sig['pair']} pump={sig['pump_pct']:+.1f}% vol×{sig['vol_ratio']:.1f}")
        time.sleep(TG_SEND_SLEEP_SEC)

    # WR summary untuk footer
    wr_line = ""
    if wr_data and wr_data.get("n_total", 0) >= WR_MIN_SAMPLE:
        wr_line = (
            f"\n📈 Winrate  : <b>{wr_data['wr']:.0%}</b>★ "
            f"({wr_data['n_win']}/{wr_data['n_total']}) "
            f"| E[PnL]: <b>{wr_data['expectancy']:+.1f}%</b>"
        )
    elif wr_data and wr_data.get("n_total", 0) > 0:
        wr_line = f"\n📊 Akumulasi data: {wr_data['n_total']}/{WR_MIN_SAMPLE} sinyal closed"

    tg(format_summary(scanned, len(state["snapshots"]), len(candidates), sent) + wr_line)
    log(f"\n✅ Done — {sent}/{len(candidates)} sinyal terkirim")


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
