"""
╔══════════════════════════════════════════════════════════════════╗
║          INDODAX PUMP SIGNAL MONITOR  v2.0  (STATEFUL)           ║
║                                                                  ║
║  Target    : Semua pair IDR microcap di Indodax                  ║
║  Stack     : Indodax Public API + pandas + Telegram              ║
║  Mode      : Stateful — snapshot harga disimpan antar run        ║
║                                                                  ║
║  Signal Logic v2 (SEMUA harus terpenuhi):                        ║
║  1. Price Pump    — harga naik ≥ PRICE_PUMP_PCT% vs snapshot lalu║
║  2. Volume Spike  — Δvol_idr ≥ VOL_SPIKE_MULT × baseline Δvol   ║
║  3. Breakout      — harga ≥ max harga di state window            ║
║  4. RSI Cross     — RSI(14) dari ≤35 ke ≥50 (dari price series)  ║
║                                                                  ║
║  STATE: STATE_FILE di-commit ke repo setelah setiap run          ║
║         → workflow perlu step git push                           ║
║                                                                  ║
║  ENV VARS (opsional):                                            ║
║    PRICE_PUMP_PCT      float   default=3.0                       ║
║    VOL_SPIKE_MULT      float   default=5.0                       ║
║    RSI_OVERSOLD        float   default=35.0                      ║
║    RSI_RECOVERY        float   default=50.0                      ║
║    VOL_IDR_MIN         float   default=5_000_000                 ║
║    VOL_IDR_MAX         float   default=2_000_000_000             ║
║    SL_PCT / TP1_PCT / TP2_PCT / TP3_PCT  float  5/5/10/20        ║
║    MAX_SIGNALS_PER_RUN int     default=5                         ║
║    MAX_SNAPSHOTS       int     default=30                        ║
║    MIN_SNAPS_SIGNAL    int     default=3                         ║
║    STATE_FILE          str     default=price_state.json          ║
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

# ══════════════════════════════════════════════════════════════════
#  CONFIG & CONSTANTS
# ══════════════════════════════════════════════════════════════════

TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID: str   = os.environ["TELEGRAM_CHAT_ID"]
_TG_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Signal thresholds
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

# State
STATE_FILE       = os.environ.get("STATE_FILE",    "price_state.json")
MAX_SNAPSHOTS    = int(os.environ.get("MAX_SNAPSHOTS",    "30"))
MIN_SNAPS_SIGNAL = int(os.environ.get("MIN_SNAPS_SIGNAL",  "3"))
MIN_SNAPS_RSI    = 16   # minimum snapshot untuk RSI(14) stabil

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

def calc_levels(entry: float) -> dict:
    if entry <= 0:
        return {}
    sl  = entry * (1 - SL_PCT  / 100)
    tp1 = entry * (1 + TP1_PCT / 100)
    tp2 = entry * (1 + TP2_PCT / 100)
    tp3 = entry * (1 + TP3_PCT / 100)
    rr  = round((tp2 - entry) / (entry - sl), 1) if entry > sl else 0.0
    return {"entry": entry, "sl": round(sl, 8),
            "tp1": round(tp1, 8), "tp2": round(tp2, 8), "tp3": round(tp3, 8),
            "rr": rr}


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


def format_signal(sig: dict) -> str:
    ts           = sig["ts"].strftime("%d/%m/%Y %H:%M WIB")
    pair_display = sig["pair"].replace("_", "/").upper()
    lvl          = calc_levels(sig["price"])

    rsi_line = (
        f"📊 RSI(14)   : <b>{sig['rsi']:.1f}</b>\n"
        if sig.get("has_rsi") and sig["rsi"] > 0
        else f"📊 RSI(14)   : <i>akumulasi ({sig['snaps']}/{MIN_SNAPS_RSI} snap)</i>\n"
    )

    if lvl:
        low_ref  = sig.get("low_24h", 0)
        low_line = f"└ Support : <code>{_fp(low_ref)}</code>  <i>(low 24h)</i>\n" if low_ref > 0 else ""
        levels = (
            f"\n🎯 <b>LEVEL TRADING</b>\n"
            f"┌ Entry  : <b>{_fp(lvl['entry'])}</b>\n"
            f"├ SL     : <code>{_fp(lvl['sl'])}</code>  <i>(-{SL_PCT:.0f}%)</i>\n"
            f"├ TP1    : <code>{_fp(lvl['tp1'])}</code>  <i>(+{TP1_PCT:.0f}%)</i>\n"
            f"├ TP2    : <code>{_fp(lvl['tp2'])}</code>  <i>(+{TP2_PCT:.0f}%)</i>\n"
            f"├ TP3    : <code>{_fp(lvl['tp3'])}</code>  <i>(+{TP3_PCT:.0f}%)</i>\n"
            f"{low_line}"
            f"⚖️ R/R   : <b>{lvl['rr']}×</b> (vs TP2)\n"
        )
    else:
        levels = ""

    return (
        f"🚨 <b>PUMP SIGNAL — {sig['coin']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💱 Pair      : <code>{pair_display}</code>\n"
        f"📈 Pump      : <b>+{sig['pump_pct']:.2f}%</b> vs snapshot lalu\n"
        f"🔊 Vol Spike : <b>{sig['vol_ratio']:.1f}×</b> baseline\n"
        f"🔓 Breakout  : harga baru ({sig['snaps']} snapshot window)\n"
        f"{rsi_line}"
        f"📦 Vol 24h   : {_fmt_idr(sig['vol_idr'])}\n"
        f"{levels}"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ {ts}\n"
        f"⚠️ <i>Bukan saran investasi. DYOR.</i>"
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
    log("=" * 64)
    log("🔍 INDODAX PUMP SIGNAL MONITOR v2.0 (STATEFUL)")
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
        if tg(format_signal(sig)):
            sent += 1
            log(f"  📤 {sig['pair']} pump={sig['pump_pct']:+.1f}% vol×{sig['vol_ratio']:.1f}")
        time.sleep(TG_SEND_SLEEP_SEC)

    tg(format_summary(scanned, len(state["snapshots"]), len(candidates), sent))
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
