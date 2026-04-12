"""
Trading Journal Bot - Telegram Bot untuk Rekap Trading Manual
Dibuat untuk: Indodax, Tokocrypto, Ajaib
Format input: /trade [exchange] [pair] [+/-pnl]
"""

import logging
import sqlite3
import os
from datetime import datetime, time
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ─────────────────────────────────────────────
# KONFIGURASI — Ganti sesuai milikmu
# ─────────────────────────────────────────────
BOT_TOKEN = "ISI_TOKEN_BOT_TELEGRAM_KAMU"
CHAT_ID   = "ISI_CHAT_ID_KAMU"          # ID Telegram kamu (bisa dapat dari @userinfobot)
DB_PATH   = "trading_journal.db"

VALID_EXCHANGES = ["indodax", "tokocrypto", "ajaib"]

# ─────────────────────────────────────────────
# SETUP LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────
def init_db():
    """Inisialisasi database SQLite."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange    TEXT    NOT NULL,
            pair        TEXT    NOT NULL,
            pnl         REAL    NOT NULL,
            timestamp   TEXT    NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def insert_trade(exchange: str, pair: str, pnl: float):
    """Simpan satu trade ke database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO trades (exchange, pair, pnl, timestamp) VALUES (?, ?, ?, ?)",
        (exchange, pair, pnl, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )
    conn.commit()
    conn.close()


def fetch_trades_today():
    """Ambil semua trade hari ini."""
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT exchange, pair, pnl, timestamp FROM trades WHERE timestamp LIKE ?",
        (f"{today}%",)
    )
    rows = c.fetchall()
    conn.close()
    return rows


def fetch_trades_week():
    """Ambil semua trade 7 hari terakhir."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT exchange, pair, pnl, timestamp FROM trades
        WHERE timestamp >= date('now', '-7 days')
        ORDER BY timestamp DESC
    """)
    rows = c.fetchall()
    conn.close()
    return rows


def fetch_all_trades():
    """Ambil semua trade (untuk /history)."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT exchange, pair, pnl, timestamp FROM trades ORDER BY timestamp DESC LIMIT 20")
    rows = c.fetchall()
    conn.close()
    return rows


# ─────────────────────────────────────────────
# HELPER: GENERATE LAPORAN
# ─────────────────────────────────────────────
def generate_report(trades: list, title: str) -> str:
    """
    Generate teks laporan dari list trade.
    trades: list of (exchange, pair, pnl, timestamp)
    """
    if not trades:
        return f"📭 *{title}*\n\nBelum ada trade yang tercatat."

    total_pnl   = sum(t[2] for t in trades)
    total_trade = len(trades)
    win_trades  = [t for t in trades if t[2] > 0]
    loss_trades = [t for t in trades if t[2] < 0]
    win_count   = len(win_trades)
    loss_count  = len(loss_trades)
    accuracy    = (win_count / total_trade * 100) if total_trade > 0 else 0

    best_trade  = max(trades, key=lambda x: x[2])
    worst_trade = min(trades, key=lambda x: x[2])

    # PnL per exchange
    exchanges = {}
    for t in trades:
        ex = t[0].capitalize()
        exchanges[ex] = exchanges.get(ex, 0) + t[2]

    exchange_lines = "\n".join(
        f"  • {ex}: {'+'if v>=0 else ''}${v:.2f}"
        for ex, v in exchanges.items()
    )

    pnl_emoji = "📈" if total_pnl >= 0 else "📉"
    pnl_str   = f"+${total_pnl:.2f}" if total_pnl >= 0 else f"-${abs(total_pnl):.2f}"

    report = (
        f"📊 *{title}*\n"
        f"{'─'*30}\n"
        f"📦 Total Trade  : {total_trade}\n"
        f"✅ Win          : {win_count}\n"
        f"❌ Loss         : {loss_count}\n"
        f"🎯 Akurasi      : {accuracy:.1f}%\n"
        f"{'─'*30}\n"
        f"{pnl_emoji} Total PnL     : *{pnl_str}*\n"
        f"📈 Best Trade   : +${best_trade[2]:.2f} ({best_trade[1].upper()} @ {best_trade[0].capitalize()})\n"
        f"📉 Worst Trade  : -${abs(worst_trade[2]):.2f} ({worst_trade[1].upper()} @ {worst_trade[0].capitalize()})\n"
        f"{'─'*30}\n"
        f"🏦 *Per Exchange:*\n{exchange_lines}\n"
    )
    return report


# ─────────────────────────────────────────────
# COMMAND HANDLERS
# ─────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler /start — tampilkan panduan penggunaan."""
    msg = (
        "👋 *Trading Journal Bot*\n\n"
        "Bot ini mencatat trade manual kamu dan merekap performa secara otomatis.\n\n"
        "📌 *Cara Input Trade:*\n"
        "`/trade [exchange] [pair] [pnl]`\n\n"
        "*Contoh:*\n"
        "`/trade indodax BTC +15`\n"
        "`/trade tokocrypto ETH -8.5`\n"
        "`/trade ajaib BBCA +20`\n\n"
        "📋 *Exchange yang tersedia:*\n"
        "• indodax\n• tokocrypto\n• ajaib\n\n"
        "📊 *Perintah Lainnya:*\n"
        "`/rekap` — Rekap hari ini\n"
        "`/minggu` — Rekap 7 hari terakhir\n"
        "`/history` — 20 trade terakhir\n"
        "`/help` — Tampilkan panduan ini\n"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


async def cmd_trade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler /trade [exchange] [pair] [pnl]
    Contoh: /trade indodax BTC +15
    """
    args = context.args

    # Validasi jumlah argumen
    if len(args) != 3:
        await update.message.reply_text(
            "⚠️ Format salah.\n\n"
            "Gunakan: `/trade [exchange] [pair] [pnl]`\n"
            "Contoh: `/trade indodax BTC +15`",
            parse_mode="Markdown"
        )
        return

    exchange_raw, pair_raw, pnl_raw = args
    exchange = exchange_raw.lower()
    pair     = pair_raw.upper()

    # Validasi exchange
    if exchange not in VALID_EXCHANGES:
        await update.message.reply_text(
            f"⚠️ Exchange *{exchange_raw}* tidak dikenal.\n\n"
            f"Exchange yang tersedia: `indodax`, `tokocrypto`, `ajaib`",
            parse_mode="Markdown"
        )
        return

    # Validasi & parse PnL
    try:
        pnl = float(pnl_raw.replace(",", "."))
    except ValueError:
        await update.message.reply_text(
            "⚠️ Format PnL salah.\n\n"
            "Gunakan angka dengan tanda + atau -\n"
            "Contoh: `+15` atau `-8.5`",
            parse_mode="Markdown"
        )
        return

    # Simpan ke database
    insert_trade(exchange, pair, pnl)

    # Konfirmasi
    pnl_str   = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
    pnl_emoji = "✅" if pnl >= 0 else "❌"
    now_str   = datetime.now().strftime("%H:%M")

    await update.message.reply_text(
        f"{pnl_emoji} *Trade Tercatat!*\n\n"
        f"🏦 Exchange : {exchange.capitalize()}\n"
        f"💱 Pair     : {pair}\n"
        f"💰 PnL      : *{pnl_str}*\n"
        f"🕐 Waktu    : {now_str}\n\n"
        f"_Ketik /rekap untuk lihat ringkasan hari ini._",
        parse_mode="Markdown"
    )


async def cmd_rekap(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler /rekap — rekap trade hari ini."""
    trades = fetch_trades_today()
    today  = datetime.now().strftime("%d %B %Y")
    report = generate_report(trades, f"Rekap Hari Ini — {today}")
    await update.message.reply_text(report, parse_mode="Markdown")


async def cmd_minggu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler /minggu — rekap 7 hari terakhir."""
    trades = fetch_trades_week()
    report = generate_report(trades, "Rekap 7 Hari Terakhir")
    await update.message.reply_text(report, parse_mode="Markdown")


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler /history — tampilkan 20 trade terakhir."""
    trades = fetch_all_trades()
    if not trades:
        await update.message.reply_text("📭 Belum ada trade yang tercatat.")
        return

    lines = ["📋 *20 Trade Terakhir*\n" + "─" * 30]
    for ex, pair, pnl, ts in trades:
        pnl_str   = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        pnl_emoji = "✅" if pnl >= 0 else "❌"
        date_str  = ts[:16]
        lines.append(f"{pnl_emoji} `{date_str}` | {ex.capitalize()} | {pair} | *{pnl_str}*")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler /help — tampilkan panduan."""
    await cmd_start(update, context)


# ─────────────────────────────────────────────
# SCHEDULED JOB: REKAP OTOMATIS TIAP MALAM
# ─────────────────────────────────────────────
async def send_daily_recap(context: ContextTypes.DEFAULT_TYPE):
    """Kirim rekap harian otomatis ke CHAT_ID setiap malam pukul 21:00."""
    trades = fetch_trades_today()
    today  = datetime.now().strftime("%d %B %Y")
    report = generate_report(trades, f"📅 Rekap Otomatis — {today}")

    await context.bot.send_message(
        chat_id=CHAT_ID,
        text=report,
        parse_mode="Markdown"
    )


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    # Inisialisasi database
    init_db()

    # Build aplikasi bot
    app = Application.builder().token(BOT_TOKEN).build()

    # Register command handlers
    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("trade",   cmd_trade))
    app.add_handler(CommandHandler("rekap",   cmd_rekap))
    app.add_handler(CommandHandler("minggu",  cmd_minggu))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("help",    cmd_help))

    # Jadwalkan rekap otomatis tiap malam pukul 21:00
    app.job_queue.run_daily(
        send_daily_recap,
        time=time(hour=21, minute=0, second=0),
        name="daily_recap"
    )

    logger.info("Bot berjalan... Tekan Ctrl+C untuk berhenti.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
