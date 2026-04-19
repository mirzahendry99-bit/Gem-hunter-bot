"""
╔══════════════════════════════════════════════════════════════════╗
║            GEM HUNTER BOT — Standalone v3.7 "Audit Complete"    ║
║                                                                  ║
║  Target    : Koin berpotensi 50x–100x di Gate.io                 ║
║  Stack     : Gate.io + Supabase + Telegram + GitHub Actions      ║
║  Timeframe : 15m (analisis utama) + 5m (konfirmasi MTF)          ║
║                                                                  ║
║  Filosofi  : Deteksi SEBELUM pump — bukan sesudah                ║
║  Trigger   : Dormancy break = flat base 2d + vol awakening 3x+   ║
║                                                                  ║
║  Changelog v2.1–v3.0: lihat git history                         ║
║                                                                  ║
║  Changelog v3.7 (dari v3.6) — AUDIT COMPLETE (FINAL FIXES):     ║
║  [G1-PARAM] calc_vol30_score() signature dibersihkan: parameter  ║
║             'client' dan 'pair' dihapus — E4 (v3.5) telah       ║
║             menghapus satu-satunya API call yang membutuhkannya. ║
║             Call site di compute_scores() juga diperbarui.      ║
║  [G2-LOG]   run_scan() kini log sesi saat ini di awal scan:      ║
║             "🔥 Prime Time" atau "🌙 Off-Peak" beserta threshold  ║
║             vol spike dan scan sleep yang aktif — memudahkan     ║
║             debugging performa dan timing produksi.              ║
║  [G3-REPORT] send_winrate_report() diperluas dengan analytics    ║
║             base_score vs bonus_score: breakdown berapa sinyal   ║
║             tiap tier yang naik karena bonus S1/S2/S3 vs murni  ║
║             kekuatan teknikal (base score). Memanfaatkan data    ║
║             yang sudah disimpan sejak F1 (v3.6).                ║
║                                                                  ║
║  Changelog v3.6 (dari v3.5) — ZERO ISSUES (AUDIT FIXES):        ║
║  [F1-SAVE]  base_score dan bonus_score ditambahkan ke save_signal ║
║             insert payload — tersedia untuk analisis retrospektif ║
║             tier inflation di Supabase.                          ║
║  [F2-SCHEMA] Supabase schema docstring diupdate: tambah kolom    ║
║             base_score INT dan bonus_score INT.                  ║
║  [F3-CONST] VOL30_CANDLE_LIMIT orphan constant dihapus — E4      ║
║             (v3.5) telah menghapus satu-satunya pemakaiannya di  ║
║             calc_vol30_score(). Tidak ada referensi tersisa.     ║
║  [F4-SLEEP] API_MICRO_SLEEP_SEC diperluas: sleep juga ditambahkan ║
║             sebelum gate 11 (get_high_90d, 4 batch calls) dan    ║
║             gate 12 (OB depth + volume wall, 2 calls). Semua     ║
║             gate berat kini mendapat jeda antar burst.           ║
║  [F5-NAME]  NEW_LISTING_MIN_CANDLES di-rename ke              ║
║             NEW_LISTING_MIN_CANDLES — sufiks _4H tidak akurat    ║
║             sejak migrasi ke 15m TF di v3.0.                    ║
║                                                                  ║
║  Changelog v3.5 (dari v3.4) — PRODUCTION READY (AUDIT FIXES):   ║
║  [E1-LIST]  NEW_LISTING_MAX_DAYS multiplier dikoreksi 6 → 96.    ║
║             Relic dari era 4h TF — "6" berarti 6 candle/hari di  ║
║             4h. Di 15m TF: 96 candle/hari. Efek sebelumnya:      ║
║             flag "new listing" hanya aktif untuk koin < 21 jam   ║
║             listing, bukan < 14 hari seperti intended.           ║
║  [E2-TREND] trend_1d field di-rename ke trend_1h di check_mtf(), ║
║             build_signal(), check_outcomes(), dan format_signal() ║
║             — nama kini akurat mencerminkan data 1h EMA yang     ║
║             dipakai, bukan 1d seperti label lama.                ║
║  [E3-ATH]   ath_dist_pct legacy field dihapus dari build_signal() ║
║             dan save_signal(). high90d_dist_pct adalah satu-      ║
║             satunya field. Tidak ada lagi duplikasi di Supabase. ║
║  [E4-VOL30] calc_vol30_score else-branch dead code dihapus.       ║
║             GEM_CANDLE_LIMIT=384 selalu >= 150 sehingga separate  ║
║             fetch tidak pernah dieksekusi. Fungsi disederhanakan: ║
║             jika prefetch tidak cukup, return 0 langsung.        ║
║  [E5-TIER]  Tier assignment menggunakan BASE score terpisah dari  ║
║             BONUS score (S1+S2+S3). Tier ditentukan dari base     ║
║             score (max 32), bonus hanya menambah display score.  ║
║             Mencegah koin base-lemah masuk MOONSHOT karena bonus. ║
║  [E6-BURST] Inter-call micro-sleep 0.05s ditambahkan di antara   ║
║             API calls dalam analyze_gem() untuk mengurangi risk   ║
║             429 burst pada Gate.io saat banyak koin lolos gate.  ║
║  [E7-SL]    sl_dist<=0 guard dikoreksi: cek dilakukan setelah    ║
║             validasi sl_pct, guard tetap ada tapi dengan komentar ║
║             yang akurat (defensive only, tidak seharusnya trigger)║
║                                                                  ║
║  Changelog v3.4 (dari v3.3) — SHARP SNIPER (AUDIT FIXES):       ║
║  [D1-MTF]   Parameter closes_4h di check_mtf() di-rename ke      ║
║             closes_15m — nama yang akurat mencerminkan TF asli   ║
║             yang dikirim (15m dari fetch_candles). Tidak ada      ║
║             perubahan logika, hanya naming clarity.              ║
║  [D2-CHNG]  Changelog v2.9 [A1-SIG] dikoreksi: vol_ratio pakai   ║
║             np.mean (bukan np.percentile(75)). Changelog lama    ║
║             tidak akurat — kode sudah benar sejak v2.9 tapi      ║
║             dokumentasinya salah. Komentar kode diupdate.        ║
║  [D3-UNIT]  GEM_CANDLE_VOL_MIN komentar dikoreksi dari "4h"       ║
║             → "15m" — unit mismatch sejak migrasi TF di v3.0.   ║
║             Downstream comments di save_signal juga dikoreksi.   ║
║  [D4-TP]    calc_dynamic_tp() redundant if/else dihapus:          ║
║             kedua branch identik, cukup satu assignment tp3       ║
║             setelah semua bracket dievaluasi.                    ║
║  [D5-SCAN]  SCAN_SLEEP_SEC prime-time adaptive: saat prime        ║
║             session (08-11 & 20-23 WIB) sleep dikurangi ke       ║
║             SESSION_SCAN_SLEEP_SEC (0.20s default) agar cycle    ║
║             scan lebih cepat saat window pump paling aktif.      ║
║             Di luar prime time tetap pakai SCAN_SLEEP_SEC normal. ║
║                                                                  ║
║  Changelog v3.3 (dari v3.2) — CLEAN SNIPER (AUDIT FIXES):       ║
║  [C1-HIGH]  get_high_90d() exception handler diperbaiki:         ║
║             jika batch loop raise exception padahal all_highs    ║
║             sudah terisi sebagian, data tidak dibuang. Cek       ║
║             len(all_highs) >= 96 sebelum fallback ke single      ║
║             fetch — mencegah loss data yang sudah valid.         ║
║  [C2-CONST] GEM_HIGH90D_CANDLE_LIMIT komentar dikoreksi dari     ║
║             "10 hari" → "fallback single-fetch cap". Konstanta   ║
║             ini hanya dipakai di fallback path get_high_90d().   ║
║  [C3-GATE]  Gate 13 (ATR minimum) dikoreksi: return None →       ║
║             return _reject("G13_atr_too_low"). Penolakan kini    ║
║             tercatat di gate_stats diagnostics — sebelumnya      ║
║             gate ini invisible di GATE REJECTION report.         ║
║  [C4-MTF]   Konstanta MTF threshold di-rename agar konsisten     ║
║             dengan field naming P2-MTF:                          ║
║             MTF_RSI_1H_MAX → MTF_RSI_FAST_MAX (threshold 5m)    ║
║             MTF_RSI_1D_MIN → MTF_RSI_MID_MIN  (threshold 1h)    ║
║             MTF_RSI_1D_MAX → MTF_RSI_MID_MAX  (threshold 1h)    ║
║  [C5-SAVE]  save_signal() ditambah kolom flat_quality_ratio      ║
║             agar data tersedia untuk analisis retrospektif v3.3+ ║
║  [C6-SCHEMA] Header docstring Supabase schema diupdate:          ║
║             tambah kolom flat_quality_ratio FLOAT (default NULL) ║
║                                                                  ║
║  Changelog v3.2 (dari v3.1) — PRECISION SNIPER (AUDIT FIXES):   ║
║  [P1-SCORE] Scoring max dikoreksi dari 37 ke 37 secara eksplisit ║
║             di semua komentar, tier threshold dinaikkan proporsional║
║             MOONSHOT=26, GEM=20, WATCH=14 (dari 22/16/11).      ║
║             run_scan() log diperbaiki dari /32 → /37.            ║
║  [P2-MTF]   Field naming di check_mtf() dikoreksi:              ║
║             rsi_fast (5m) dan rsi_mid (1h) — tidak lagi          ║
║             pakai rsi_1h/rsi_1d yang misleading. Downstream      ║
║             format_signal() ikut dikoreksi.                      ║
║  [P3-HIGH]  get_high_90d() dikoreksi — fetch multi-batch         ║
║             hingga 4×1000 candle (≈40 hari). Renamed constant    ║
║             GEM_HIGH90D_FETCH_BATCHES=4. Fungsi dan konstanta    ║
║             tetap bernama "90d" tapi kini mendekati 40 hari      ║
║             (max Gate.io limit tanpa auth waktu panjang).        ║
║  [P4-MCAP]  Auto-upgrade tier ke MOONSHOT dari est. MCap        ║
║             DIHAPUS dari build_signal(). mcap_label tetap ada    ║
║             sebagai display dan score bonus saja.                ║
║  [P5-FLAT]  flat_w shrinking dibatasi: minimum flat window       ║
║             sekarang GEM_FLAT_MIN_LEN = 72 candle (18 jam),     ║
║             tidak bisa di-shrink lebih kecil. Koin dengan        ║
║             candle history pendek di-reject lebih tegas.         ║
║  [P6-PUMP]  PUMP_CANDLE_BODY_PCT diturunkan 28% → 20% untuk    ║
║             menangkap pump candle yang lebih realistis di        ║
║             small-cap. Threshold 28% terlalu permissif.          ║
║  [P7-WALL]  OB_WALL_MIN_USDT sekarang dinamis: max($500,        ║
║             candle_vol_median × 0.3) — tidak lagi flat $1000     ║
║             yang tidak bermakna untuk koin sangat sepi.          ║
║  [P8-SESS]  Session-aware scanning: GEM_VOL_SPIKE_MIN           ║
║             dilonggarkan ke 2.5× selama jam prime time Asia      ║
║             (08:00–11:00 dan 20:00–23:00 WIB) untuk menangkap   ║
║             akumulasi awal lebih cepat.                          ║
║  [P9-FLAT]  Flat quality filter tambahan: minimal 80% candle     ║
║             dalam flat window harus punya range < GEM_FLAT_MAX_BODY.║
║             Mencegah koin dengan 1 candle anomali di tengah      ║
║             flat period lolos karena "rata-rata" masih oke.      ║
║                                                                  ║
║  Changelog v3.1 (dari v3.0) — INSTITUTIONAL SNIPER:             ║
║  [S1-FREQ]  Trade Frequency Filter                               ║
║  [S2-MCAP]  Micro Market Cap Scanner                             ║
║  [S3-WALL]  Volume Wall Detection                                ║
║                                                                  ║
║  Changelog v3.0 (dari v2.9) — FAST-GEM UPGRADE:                 ║
║  [F1-SPEED] TIMEFRAME_MAIN diubah 4h → 15m; TIMEFRAME_CONFIRM   ║
║             diubah 1h → 5m. Bot deteksi pump dalam menit,        ║
║             bukan menunggu candle 4 jam selesai.                 ║
║  [F2-DORM]  MIN_DORMANCY_PERIODS setara 2 hari (192 candle 15m); ║
║             GEM_FLAT_MAX_BODY dilonggarkan 2.0% → 2.5%;          ║
║             MAX_DORMANCY_VOLATILITY dilonggarkan 0.15 → 0.25     ║
║  [F3-VOL]   VOL_AWAKENING_RATIO diturunkan 8.0 → 3.0 — deteksi  ║
║             awal sebelum volume "meledak"; baseline kembali ke   ║
║             np.mean() dari np.percentile(75) agar lebih sensitif ║
║  [F4-BTC]   get_btc_regime() dilonggarkan: bot tidak halt kecuali ║
║             BTC turun >5% dalam 4 jam (bukan block saat sideways)║
║  [F5-EXIT]  Trailing Take Profit: ambil modal di TP1 (10–20%),   ║
║             geser SL ke entry setelah TP1 tercapai — "tiket      ║
║             gratis" untuk menunggu TP2/TP3 tanpa risiko modal    ║
║                                                                  ║
║  Changelog v2.9 (dari v2.8) — AUDIT FIXES:                      ║
║  [A1-SIG]   vol_ratio baseline: np.mean() dipakai untuk flat      ║
║             period baseline (lebih stabil dari np.max).           ║
║             Breakout window: np.mean() dari 4 candle terakhir.   ║
║             Catatan: changelog sebelumnya menyebut np.percentile  ║
║             (75) — ini tidak akurat, kode selalu pakai np.mean.  ║
║             [D2-CHNG v3.4] Dikoreksi.                            ║
║  [A2-SIG]   get_ath_price() di-rename → get_high_90d(); semua   ║
║             referensi ATH di-rename ke HIGH_90D; docstring       ║
║             menjelaskan scope 90-hari; TP3 dikap pada            ║
║             entry × (high_90d / entry) × 1.5 untuk ath_dist≥0.85║
║  [A3-SIG]   RNDR dihapus dari sektor AI — hanya ada di DEPIN;   ║
║             mencegah RNDR selalu resolve sebagai AI dan tidak     ║
║             pernah dapat DEPIN sector bonus                       ║
║  [A4-RISK]  format_signal() kini tampilkan alokasi modal tier-   ║
║             aware: MOONSHOT 0.25–0.5%, GEM 0.5–1%, WATCH         ║
║             0.25–0.5%; ultra_low_liq mendapat warning tambahan   ║
║  [A5-EXEC]  get_btc_regime() exception handler diubah ke         ║
║             fail-safe: halt=True jika data BTC tidak tersedia    ║
║  [A6-EXEC]  check_outcomes() batch ticker fetch: satu panggilan  ║
║             list_tickers() untuk semua USDT, resolve dari map    ║
║             — eliminasi N individual API call per pending sinyal  ║
║  [A7-QUAL]  Outcome persistence block di check_outcomes() kini   ║
║             dibungkus try-except agar kegagalan Supabase tidak   ║
║             menyebabkan sinyal ter-evaluasi ulang tanpa henti     ║
║  [A8-QUAL]  WINRATE_REPORT_HOUR, MAX_SIGNALS_PER_DAY, dan       ║
║             TG_OUTCOME_SLEEP_SEC sekarang environment-driven      ║
║             dengan os.environ.get() — operasional tanpa redeploy ║
║  [A9-QUAL]  analyze_gem() didekomposisi menjadi tiga fungsi:     ║
║             run_gate_filters(), compute_scores(), build_signal() ║
║             — masing-masing ~60–80 baris, complexity turun       ║
║  [A10-EDGE] check_outcomes() ambil candle high 4h terkini untuk  ║
║             update watermark — tangkap spike intra-run yang       ║
║             sebelumnya tidak terdeteksi karena hanya pakai last  ║
║                                                                  ║
║  Changelog v2.5 (dari v2.4) — AUDIT FIXES:                      ║
║  [FIX-E1]  Version label di run_scan() dikoreksi ke v2.5        ║
║  [FIX-E2]  R/R dihitung dari TP2 (bukan TP1); GEM_MIN_RR        ║
║            dinaikkan 5.0 → 8.0 — filter ini kini bermakna        ║
║  [FIX-E3]  PUMP_CANDLE_BODY_PCT dinaikkan 15% → 28%;            ║
║            15% terlalu rendah → memblokir legitimate breakout    ║
║  [FIX-E4]  Prefix fallback di get_narrative_bonus() DIHAPUS;    ║
║            "BOTANIC" tidak lagi false-match "BOT" dll            ║
║  [FIX-E5]  GEM_FLAT_MAX_BODY diperketat 3.0 → 2.0%;            ║
║            koin 2.8% range bukan dormancy sejati                 ║
║  [FIX-E6]  Minimum ATR% filter (GEM_ATR_PCT_MIN = 0.5%);        ║
║            ATR sangat rendah = koin zombie atau illiquid palsu   ║
║  [FIX-E7]  Stale breakout filter: dist>20% + 24h<5% = skip;     ║
║            momentum sudah habis, setup sudah lewat               ║
║  [FIX-E8]  Minimum candle volume median $200/15m candle;         ║
║            [D3-UNIT v3.4] dikoreksi dari "4h" → "15m"           ║
║            koin di bawah ini tidak bisa di-exit tanpa slippage   ║
║  [FIX-E9]  ATH dead-project filter diperkuat: koin >95% dari     ║
║            ATH harus punya vol median >$500/candle (8 candle)    ║
║  [OPT-E1]  calc_vol30_score kini gunakan prefetched_volumes;     ║
║            eliminasi 1 duplicate API call per pair di-scan       ║
║                                                                  ║
║  Changelog v2.6 (dari v2.5) — AUDIT FIXES:                      ║
║  [ISU-1]  save_signal() sekarang menyimpan candle_vol_median     ║
║           ke Supabase — data tersedia untuk analisis retrospektif║
║  [ISU-2]  VOL_WEAKENING: baseline diganti dari median window     ║
║           terkini → vol puncak sinyal (vol_ratio × flat_median)  ║
║           Alert hanya muncul ketika momentum breakout benar-benar║
║           melemah vs vol saat sinyal, bukan vs candle biasa      ║
║  [ISU-3]  PUMP_CANDLE_BODY_MIN = 0.85 dihapus — orphan constant ║
║           tidak dipakai sejak v2.3, membingungkan pembaca kode   ║
║  [ISU-4]  age_days kini pakai total_seconds()/86400 bukan .days  ║
║           — .days memotong jam sehingga sinyal malam salah hitung║
║  Changelog v2.8 (dari v2.7) — AUDIT FIXES:                      ║
║  [KRITIS-1] atr() sekarang pakai 14 candle TERBARU ([-15:-1])   ║
║             bukan candle tertua — ATR kini cerminkan volatilitas ║
║             terkini, bukan flat dormancy 28–56 hari lalu         ║
║  [KRITIS-2] send_winrate_report() cek dedup di gem_dedup agar   ║
║             tidak terkirim >1x per hari saat GitHub Actions      ║
║             berjalan setiap 30 menit                             ║
║  [MEDIUM-3] calc_vol30_score() pakai prefetched 168c (7 hari),  ║
║             bukan 30 hari — sekarang selalu fetch 180c terpisah  ║
║             untuk baseline yang benar, kecuali prefetched ≥ 150 ║
║  [MEDIUM-4] check_mtf() sekarang set ok=False jika API gagal    ║
║             fetch 1h atau 1d — koin tidak lolos MTF saat down    ║
║  [MEDIUM-5] save_signal()+mark_sent() diperkuat: dedup_cache    ║
║             juga diisi dari gem_signals di load_dedup_cache()    ║
║  [MEDIUM-6] flat_vol_median disimpan ke gem_signals saat sinyal  ║
║             dibuat — check_outcomes pakai data DB, bukan estimasi║
║  [MINOR-7]  tg() log error final setelah 3 retry gagal semua    ║
║  [MINOR-8]  is_manipulative_pump() vol_confirm=False jika        ║
║             avg_vol_recent==0 — hindari false positive           ║
║  [MINOR-9]  check_mtf() dipindah sebelum get_ath_price() di     ║
║             analyze_gem() — eliminasi 2160c fetch jika MTF gagal ║
║                                                                  ║
║  SUPABASE SCHEMA BARU (v2.8):                                    ║
║  flat_vol_median   FLOAT   (default = NULL)  ← BARU di v2.8     ║
║                                                                  ║
║  Changelog v2.7 (dari v2.6) — AUDIT FIXES:                      ║
║  [A4-KRITIS] check_outcomes SELECT tidak include vol_ratio —     ║
║              ISU-2 fix selalu fallback ke estimasi kasar.        ║
║              vol_ratio sekarang di-fetch dari DB dengan benar    ║
║  [A2-SEDANG] Komentar FLAT_DOWNTREND_SLOPE_MAX salah unit:       ║
║              linear_slope() return %/candle, bukan total drift.  ║
║              Komentar dikoreksi agar tidak menyesatkan           ║
║  [A7-MINOR]  get_usdt_idr() tidak guard rate <= 0. Ditambah      ║
║              guard: raise ValueError jika CoinGecko return 0    ║
║  [A9-MINOR]  BLOCKED_SUFFIXES tidak cover 4L/4S. Ditambah        ║
║              "4L_USDT", "4S_USDT" — Gate.io punya produk 4x     ║
║                                                                  ║
║  candle_vol_median  FLOAT   (default = NULL)  ← BARU di v2.6    ║
║                                                                  ║
║  SUPABASE SCHEMA — pastikan kolom berikut ada di tabel gem_signals:
║  high_watermark    FLOAT   (default = NULL)                      ║
║  vol30_score       INT     (default = 0)                         ║
║  narrative         TEXT    (default = NULL)                      ║
╚══════════════════════════════════════════════════════════════════╝

SUPABASE SCHEMA — pastikan kolom berikut ada di tabel gem_signals:
  high_watermark      FLOAT   (tambah jika belum ada, default = NULL)
  vol30_score         INT     (tambah jika belum ada, default = 0)
  narrative           TEXT    (tambah jika belum ada, default = NULL)
  candle_vol_median   FLOAT   (tambah jika belum ada, default = NULL)  ← BARU v2.6
  flat_vol_median     FLOAT   (tambah jika belum ada, default = NULL)  ← BARU v2.8
  flat_quality_ratio  FLOAT   (tambah jika belum ada, default = NULL)  ← BARU v3.3 [C6-SCHEMA]
  base_score          INT     (tambah jika belum ada, default = NULL)  ← BARU v3.6 [F2-SCHEMA]
  bonus_score         INT     (tambah jika belum ada, default = NULL)  ← BARU v3.6 [F2-SCHEMA]

ENV VARS BARU v2.9 (opsional — semua punya default):
  WINRATE_REPORT_HOUR     int   default=8
  MAX_SIGNALS_PER_DAY     int   default=8
  TG_OUTCOME_SLEEP_SEC    float default=1.0
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
SCAN_SLEEP_SEC = float(os.environ.get("SCAN_SLEEP_SEC", "0.40"))

# ── BTC Market Regime ─────────────────────────────────────────────
# [F4-BTC] BTC_HALT_PCT diperketat kondisi halt: bot berhenti hanya
# jika BTC benar-benar crash parah (>8% dalam 4 jam, tidak berubah).
# BTC_BLOCK_PCT dilonggarkan: -4% → tidak lagi memblokir selama BTC
# sideways. Koin micin sering pump justru saat BTC boring/sideways.
# Bot tetap bekerja selama BTC tidak crash >5% dalam window pendek.
BTC_HALT_PCT  = -8.0   # halt total — BTC crash parah
BTC_BLOCK_PCT = -5.0   # [F4-BTC] dilonggarkan -4% → -5%; tidak block saat BTC sideways

# ── Volume Filter ──────────────────────────────────────────────────
GEM_VOL_MIN       = 500
GEM_VOL_MAX       = 500_000   # [WEAK-1] dinaikkan dari 150K → 500K (exit lebih realistis)
GEM_VOL_MAX_ULTRA = 150_000   # flag display: koin di bawah ini ditandai "ULTRA LOW LIQ"

# ── Candle ────────────────────────────────────────────────────────
# [F1-SPEED] Timeframe utama diubah 4h → 15m agar pump terdeteksi
# dalam menit sejak dimulai, bukan setelah candle 4 jam selesai.
# TIMEFRAME_CONFIRM (untuk MTF) diubah 1h → 5m.
GEM_TF              = "15m"
GEM_TF_CONFIRM      = "5m"     # [F1-SPEED] konfirmasi cepat
GEM_CANDLE_LIMIT    = 384      # 384 × 15m = 4 hari data (cukup untuk flat window + breakout)

# ── Dormancy ──────────────────────────────────────────────────────
# [F2-DORM] MIN_DORMANCY_PERIODS = 192 candle × 15m = 2 hari.
# Koin yang sedang konsolidasi singkat (1–2 hari) tetap terdeteksi.
# GEM_FLAT_WINDOW = 192 candle (2 hari) sebagai baseline flat window.
# GEM_FLAT_MAX_BODY dilonggarkan 2.0% → 2.5% — koin micin seringkali
# bergerak sedikit saat "istirahat", threshold ketat membuatnya lolos begitu saja.
GEM_FLAT_WINDOW   = 192   # [F2-DORM] 192 × 15m = 2 hari
GEM_FLAT_MAX_BODY = 2.5   # [F2-DORM] dilonggarkan 2.0% → 2.5%
GEM_FLAT_MIN_LEN  = 72    # [P5-FLAT] dinaikkan 48 → 72 candle × 15m = 18 jam minimum flat window
                           # Koin dengan history pendek yang shrink di bawah 72 candle di-reject lebih tegas

# ── Breakout ──────────────────────────────────────────────────────
# [F3-VOL] GEM_VOL_SPIKE_MIN diturunkan 8.0 → 3.0.
# Di bursa lokal, kenaikan volume 3× lipat sudah menandakan dimulainya
# pump. Threshold 8× hanya akan terdeteksi saat pump sudah setengah jalan.
GEM_BREAKOUT_WINDOW    = 4     # [F1-SPEED] 4 × 15m = 1 jam breakout window
GEM_VOL_SPIKE_MIN      = 3.0   # [F3-VOL] diturunkan 8.0 → 3.0
GEM_BREAKOUT_PCT_MIN   = 2.0   # [F1-SPEED] dilonggarkan karena TF lebih kecil
GEM_MAX_DIST_FROM_BASE = 50.0

# ── RSI ───────────────────────────────────────────────────────────
GEM_RSI_MIN = 32
GEM_RSI_MAX = 65

# ── 90-Day High (sebelumnya mislabeled sebagai "ATH") [A2] ───────
# PENTING: konstanta ini bukan "All-Time High" melainkan puncak 90 hari terakhir.
# Label di-rename dari GEM_ATH_* → GEM_HIGH90D_* agar tidak menyesatkan.
# Downstream calc_dynamic_tp() menggunakan nilai ini hanya sebagai referensi
# "ruang ke puncak terakhir yang observable", bukan ruang ke ATH sejati.
GEM_HIGH90D_DIST_MIN     = 0.30
GEM_HIGH90D_DIST_MAX     = 0.97   # [BUG-3] koin turun >97% dari 90d high dianggap sudah mati
GEM_HIGH90D_CANDLE_LIMIT = 1000   # [C2-CONST] Fallback single-fetch cap (Gate.io max).
                                   # Hanya dipakai di fallback path get_high_90d() jika
                                   # multi-batch gagal. ≈10 hari coverage (1000 × 15m).

# ── 24h Guard ─────────────────────────────────────────────────────
GEM_MAX_CHANGE_24H = 40.0

# ── Risk Management [FIX-4] ───────────────────────────────────────
GEM_SL_ATR_MULTIPLIER = 1.5
GEM_SL_MIN_PCT        = 0.05
GEM_SL_MAX_PCT        = 0.15
GEM_MIN_RR            = 8.0   # [FIX-E2] dinaikkan 5.0 → 8.0; dihitung dari TP2 bukan TP1
GEM_ATR_PCT_MIN       = 0.5   # [FIX-E6] minimum ATR% — sinyal pada koin ATR sangat rendah biasanya ilusi
GEM_DIST_STALE_MAX    = 20.0  # [FIX-E7] dist_from_base > 20% + 24h change < 5% = momentum sudah habis
GEM_CANDLE_VOL_MIN    = 200.0 # [D3-UNIT] median volume per candle 15m minimum $200 USDT
                               # (komentar sebelumnya salah tulis "4h" — TF sudah migrasi ke 15m sejak v3.0)

# ── Scoring Tiers [P1-SCORE] — max score 37 ──────────────────────
# Breakdown: dormancy(3)+vol(3)+breakout(3)+rsi(2)+ath(3)+ema(2)
#            +mtf_bonus(3)+tf_alignment(3)+sweep(2)+macd(1)
#            +depth(2)+sector(2)+vol30(2)+narrative(1) = 32 (base)
#            +freq(2)+mcap(1)+wall(2) = 37 (total dengan S1/S2/S3)
#
# [P1-SCORE] v3.1 threshold ditetapkan untuk score max 32 tapi
# scoring sudah mencapai 37 sejak S1/S2/S3 ditambahkan.
# v3.2: threshold dinaikkan proporsional (×37/32):
#   MOONSHOT: 22 → 26  |  GEM: 16 → 20  |  WATCH: 11 → 14
GEM_TIER: dict[str, int] = {"MOONSHOT": 26, "GEM": 20, "WATCH": 14}

# ── MTF ───────────────────────────────────────────────────────────
MTF_ENABLED        = True
# [C4-MTF] Di-rename agar konsisten dengan field rsi_fast/rsi_mid (P2-MTF v3.2):
#   MTF_RSI_1H_MAX → MTF_RSI_FAST_MAX  (threshold untuk TF cepat = 5m)
#   MTF_RSI_1D_MIN → MTF_RSI_MID_MIN   (threshold untuk TF medium = 1h)
#   MTF_RSI_1D_MAX → MTF_RSI_MID_MAX   (threshold untuk TF medium = 1h)
MTF_RSI_FAST_MAX = 72   # RSI 5m maksimum — overbought di TF cepat = reject
MTF_RSI_MID_MIN  = 30   # RSI 1h minimum — terlalu oversold di TF medium = reject
MTF_RSI_MID_MAX  = 75   # RSI 1h maksimum — overbought di TF medium = reject

# ── Liquidity Spread ──────────────────────────────────────────────
LIQ_SPREAD_MAX_PCT = 3.0
LIQ_ENABLED        = True

# ── Order Book Depth [UPG-1] ──────────────────────────────────────
OB_DEPTH_LEVELS       = 20
OB_DEPTH_WINDOW_PCT   = 0.05   # 5% dari mid price
OB_DEPTH_RATIO_HIGH   = 2.0    # bid dominasi kuat → score 2
OB_DEPTH_RATIO_MED    = 1.3    # bid sedikit dominan → score 1

# ── S1: Trade Frequency Filter [S1-FREQ] ──────────────────────────
# Deteksi "kebocoran" aktivitas bot dari exchange lain (Indodax/Tokocrypto)
# sebelum harganya terbang ke Gate.io. Koin tidur biasanya cuma punya
# 1–3 tx/menit. Lonjakan mendadak ke 20+ tx/menit = bot exchange lain
# mulai akumulasi. Kita beli sebelum harga naik.
#
# Gate.io API: list_trades() mengembalikan recent trades dengan timestamp.
# Kita ambil 60 detik terakhir dan hitung berapa transaksi yang terjadi.
TRADE_FREQ_WINDOW_SEC  = 60    # window observasi dalam detik
TRADE_FREQ_FETCH_LIMIT = 100   # max trades yang di-fetch (Gate API limit)
TRADE_FREQ_SPIKE_MIN   = 15    # [S1] ≥15 tx/menit = aktivitas tidak normal
TRADE_FREQ_STRONG      = 30    # [S1] ≥30 tx/menit = bot exchange lain aktif
TRADE_FREQ_ENABLED     = True  # matikan jika ingin skip (hemat API call)

# ── S2: Micro Market Cap Filter [S2-MCAP] ─────────────────────────
# Koin yang bisa naik 100x HANYA koin dengan MCap sangat kecil.
# SOL atau AVAX mustahil 100x — capitalnya sudah terlalu besar.
# Filter ini fokus pada koin "micro" yang masih bisa terbang.
#
# Estimasi MCap = price × estimated_circulating_supply
# Supply diestimasi dari vol_24h dengan asumsi turnover rate.
# Label "ULTRA GEM" muncul di Telegram jika est. MCap < threshold.
MCAP_ULTRA_GEM_USD    = 5_000_000    # [S2] est. MCap < $5 Juta → ULTRA GEM
MCAP_GEM_USD          = 20_000_000   # [S2] est. MCap < $20 Juta → GEM tier
MCAP_PRICE_MAX        = 0.10         # [S2] harga satuan maks $0.10 untuk label micro
MCAP_TURNOVER_ASSUMED = 0.10         # [S2] asumsi 10% MCap diperdagangkan per hari

# ── S3: Volume Wall Consistency [S3-WALL] ─────────────────────────
# Anti Fake Pump: cross-validate antara volume spike dan ketebalan bid wall.
# Wash trading biasanya menghasilkan volume besar tapi bid wall tipis
# (pelaku jual beli sendiri tanpa meninggalkan jejak beli yang nyata).
# Sinyal valid = volume spike + bid wall tebal terkonfirmasi bersama.
#
# OB_WALL_MIN_USDT: nilai minimum bid wall dalam window 2% untuk dianggap "tebal"
# OB_WALL_VOL_RATIO: bid wall harus setidaknya sekian × median candle vol
OB_WALL_MIN_USDT      = 1_000   # [S3] bid wall minimum $1000 USDT dalam window 2%
OB_WALL_VOL_RATIO     = 0.5     # [S3] bid wall ≥ 0.5× median candle vol = legit
OB_WALL_WINDOW_PCT    = 0.02    # [S3] 2% di bawah mid price untuk cek support wall

# ── Sector Rotation [UPG-2] ───────────────────────────────────────
SECTOR_HOT_THRESHOLD = 15.0    # % 24h change untuk dianggap "hot"
SECTOR_HOT_MIN_COUNT = 2       # minimal 2 coin sektor naik

SECTOR_MAP: dict[str, list[str]] = {
    # Narasi established
    # [A3] RNDR dihapus dari AI — hanya ada di DEPIN agar RNDR bisa mendapat
    # DEPIN sector bonus saat narasi DePIN sedang hot (sebelumnya selalu resolve
    # sebagai AI karena loop break on first match, dan AI muncul lebih dulu)
    "AI":       ["FET", "AGIX", "OCEAN", "NMR", "TAO", "WLD", "GRT"],
    "L2":       ["ARB", "OP", "IMX", "BOBA", "METIS", "MANTA", "BLAST", "SCROLL", "ZK", "STRK"],
    "DEFI":     ["AAVE", "UNI", "CRV", "BAL", "SUSHI", "COMP", "MKR", "SNX", "PENDLE", "SKY"],
    "GAMEFI":   ["AXS", "SAND", "MANA", "ENJ", "GALA", "ILV", "YGG", "MAGIC", "BEAM", "RON"],
    "MEME":     ["DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "BRETT", "MOG", "POPCAT", "NEIRO"],
    "RWA":      ["ONDO", "CFG", "MPL", "TRU", "CPOOL", "POLYX", "PAXG", "CHEX"],
    "INFRA":    ["LINK", "DOT", "ATOM", "AVAX", "INJ", "SEI", "SUI", "APT", "TON", "NEAR"],
    "PRIVACY":  ["XMR", "ZEC", "SCRT", "NYM", "AZT", "RAIL"],
    # Narasi 2025–2026 [WEAK-3]
    "AI_AGENT": ["VIRTUAL", "AI16Z", "AIXBT", "GOAT", "ARC", "VADER", "LUNA", "ACT"],
    "DEPIN":    ["IO", "RNDR", "HNT", "MOBILE", "WIFI", "GRASS", "NATIX", "DIMO"],
    "BTCFI":    ["STX", "RUNE", "ORDI", "SATS", "ALEX", "MERLIN", "BOB", "BABYLON"],
    "RESTAKE":  ["EIGEN", "LRT", "ETHFI", "RSETH", "PUFFER", "SWELL", "KELP"],
    "SOLANA_ECO": ["JTO", "JUP", "PYTH", "WEN", "BOME", "SLERF", "MEW", "KMNO"],
}

# ── Narrative Keyword Bonus [UPG-C2] ─────────────────────────────
# Koin yang namanya mengandung narasi aktif 2025–2026 mendapat bonus +1
# Ini proxy sederhana tapi efektif untuk momentum naratif
# [FIX-D5] Exact-match set (bukan substring) — hindari false match
# mis: "REAL" substring bisa match ke koin tidak relevan seperti "SURREAL"
# Gunakan set untuk O(1) lookup dan exact token match setelah tokenisasi
NARRATIVE_KEYWORDS: set[str] = {
    # AI narrative
    "AI", "GPT", "AGI", "BOT", "NEURAL", "MIND",
    # BTCfi narrative
    "BTC", "SATOSHI", "ORDI", "RUNE", "ORDINAL",
    # RWA narrative
    "RWA", "GOLD", "BOND", "TREASURY",
    # DePIN narrative
    "DEPIN", "WIFI", "MESH", "HOTSPOT",
    # Restaking narrative
    "EIGEN", "RESTAKE", "LIQUID",
    # High-performance L1
    "SOL", "SUI", "APT", "TON", "MONAD",
    # AI Agent narrative
    "AGENT", "VIRTUAL", "SWARM", "GOAT",
}

# ── Post-Signal Volume Alert [UPG-C4] ─────────────────────────────
VOL_WEAKENING_RATIO = 3.0  # jika vol drop di bawah 3× baseline → kirim alert

# ── Anti-Pump ─────────────────────────────────────────────────────
PUMP_VOL_SPIKE_MAX   = 200.0
PUMP_PRICE_SPIKE_MAX = 80.0
# PUMP_CANDLE_BODY_MIN dihapus di v2.6 — tidak dipakai sejak v2.3 [ISU-3]
PUMP_CANDLE_BODY_PCT  = 20.0   # [P6-PUMP] diturunkan 28% → 20%; 28% terlalu permissif untuk small-cap
                                # koin microcap breakout legitim bisa naik 15-20% per candle 15m

# ── Win Rate Report [FIX-8] ───────────────────────────────────────
# [A8] Environment-driven agar bisa diubah tanpa redeploy kode
WINRATE_REPORT_HOUR = int(os.environ.get("WINRATE_REPORT_HOUR", "8"))
WINRATE_MIN_SIGNALS = 5

# ── New Listing Guard [FIX-6] ─────────────────────────────────────
# [F1-SPEED] Minimum candle guard: 288 × 15m = 3 hari
# [E1-LIST]  NEW_LISTING_MAX_DAYS multiplier dikoreksi 6 → 96.
#            6 adalah relic era 4h TF (6 candle/hari × 4h = 24h).
#            Di 15m TF: 96 candle/hari. Flag "new listing" sebelumnya
#            hanya aktif untuk koin < 84 candle = 21 jam, bukan 14 hari.
NEW_LISTING_MIN_CANDLES = 288   # [F5-NAME] 3 hari × 96 candle/hari = 288 candle 15m (renamed dari NEW_LISTING_MIN_CANDLES_4H)
NEW_LISTING_MAX_DAYS       = 14    # flag display only (bukan bonus score)
NEW_LISTING_CANDLES_PER_DAY = 96   # [E1-LIST] 96 × 15m = 1 hari (bukan 6 × 4h)

# ── Flat Base Slope Filter [FIX-B7] — disesuaikan untuk TF 15m ───
# [F1-SPEED] Slope threshold dilonggarkan sedikit karena noise candle
# 15m lebih tinggi dari 4h — slope -0.05%/candle terlalu ketat di TF kecil.
FLAT_DOWNTREND_SLOPE_MAX    = -0.02   # [F1-SPEED] -0.05 → -0.02 %/candle untuk 15m
GEM_FLAT_MIN_LEN_BTC_BLOCK  = 120     # [P5-FLAT] dinaikkan 96 → 120 × 15m = 30 jam, lebih ketat saat BTC drop

# ── Telegram Rate Limit [FIX-9, FIX-10] ──────────────────────────
# [A8] Environment-driven agar bisa diubah tanpa redeploy kode
TG_OUTCOME_SLEEP_SEC = float(os.environ.get("TG_OUTCOME_SLEEP_SEC", "1.0"))

# ── Dedup & Limits ────────────────────────────────────────────────
GEM_DEDUP_HOURS     = 48
MAX_SIGNALS_PER_RUN = 3
# [A8] Environment-driven agar bisa diubah tanpa redeploy kode
MAX_SIGNALS_PER_DAY = int(os.environ.get("MAX_SIGNALS_PER_DAY", "8"))

# ── Session-Aware Scanning [P8-SESS] ──────────────────────────────
# Jam prime time Asia: overlap Indodax/Tokocrypto aktif + Eropa mulai buka
# Vol spike threshold dilonggarkan saat jam ini untuk tangkap akumulasi awal
SESSION_PRIME_HOURS: tuple[tuple[int,int], ...] = ((8, 11), (20, 23))  # WIB
SESSION_VOL_SPIKE_RELAXED = 2.5   # [P8-SESS] lebih sensitif saat prime time (vs 3.0 normal)
SESSION_SCAN_SLEEP_SEC    = float(os.environ.get("SESSION_SCAN_SLEEP_SEC", "0.20"))
# [D5-SCAN] Sleep antar ticker dikurangi ke 0.20s saat prime session (08-11 & 20-23 WIB)
# agar scan cycle selesai lebih cepat saat window pump paling aktif.
# Di luar prime time tetap pakai SCAN_SLEEP_SEC (0.40s default) untuk hemat rate limit.

# ── Inter-call micro-sleep [E6-BURST] ─────────────────────────────
# Jeda singkat antar API calls di dalam analyze_gem() untuk mengurangi
# burst 429 pada Gate.io ketika banyak koin lolos gate awal sekaligus.
API_MICRO_SLEEP_SEC = float(os.environ.get("API_MICRO_SLEEP_SEC", "0.05"))
# Minimal 80% candle dalam flat window harus punya range < GEM_FLAT_MAX_BODY
# Mencegah koin dengan 1-2 candle anomali besar di tengah flat period lolos
FLAT_QUALITY_MIN_RATIO = 0.80   # [P9-FLAT] 80% candle harus dalam batas flat

# ── Blocked pairs ─────────────────────────────────────────────────
BLOCKED_SUFFIXES = (
    "2L_USDT", "2S_USDT", "3L_USDT", "3S_USDT",
    "4L_USDT", "4S_USDT",                          # [A9] ditambah — Gate.io punya produk 4x
    "5L_USDT", "5S_USDT", "UP_USDT", "DOWN_USDT",
    "BULL_USDT", "BEAR_USDT",
    "USDC_USDT", "BUSD_USDT", "DAI_USDT", "TUSD_USDT",
    "USDD_USDT", "FRAX_USDT", "USDP_USDT",
    "LUSD_USDT", "USTC_USDT",
)


# ══════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════

def log(msg: str, level: str = "info") -> None:
    ts    = datetime.now(WIB).strftime("%H:%M:%S")
    icons = {"info": "·", "warn": "⚠", "error": "✖", "ok": "✔"}
    print(f"[{ts}] {icons.get(level, '·')} {msg}", flush=True)


# ══════════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════════

def tg(msg: str) -> None:
    """Kirim pesan HTML ke Telegram dengan retry + handle 429."""
    url     = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML",
                "disable_web_page_preview": True}
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=15)
            if r.status_code == 200:
                return
            if r.status_code == 429:
                wait = r.json().get("parameters", {}).get("retry_after", 5)
                log(f"TG 429 — tunggu {wait}s", "warn")
                time.sleep(wait)
                continue
            log(f"TG HTTP {r.status_code}: {r.text[:120]}", "warn")
        except Exception as e:
            log(f"TG attempt {attempt+1}: {e}", "warn")
        time.sleep(2 ** attempt)
    # [MINOR-7] Log kegagalan final agar ada jejak di stdout saat pesan penting gagal terkirim
    log(f"TG FINAL FAIL — pesan gagal terkirim setelah 3 retry: {msg[:80]}...", "error")


# ══════════════════════════════════════════════════════════════════
#  GATE.IO CLIENT
# ══════════════════════════════════════════════════════════════════

def build_gate_client() -> SpotApi:
    return SpotApi(ApiClient(Configuration(key=GATE_API_KEY, secret=GATE_API_SECRET)))


def gate_retry(func, *args, retries: int = 4, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except gate_api.exceptions.ApiException as e:
            if e.status == 429:
                wait = 4 * (attempt + 1)
                log(f"Rate limit — sleep {wait}s", "warn")
                time.sleep(wait)
            elif e.status in (500, 502, 503):
                log(f"Gate {e.status} — retry", "warn")
                time.sleep(3)
            else:
                log(f"Gate API {e.status}: {e.reason}", "warn")
                return None
        except Exception as e:
            log(f"Gate error: {e}", "warn")
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
    return not any(pair.endswith(b) for b in BLOCKED_SUFFIXES)


# ══════════════════════════════════════════════════════════════════
#  CANDLE DATA
# ══════════════════════════════════════════════════════════════════

def fetch_candles(
    client: SpotApi, pair: str, tf: str = GEM_TF, limit: int = GEM_CANDLE_LIMIT
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray] | None:
    raw = gate_retry(client.list_candlesticks, currency_pair=pair, interval=tf, limit=limit)
    if not raw or len(raw) < 20:
        return None
    try:
        closes  = np.array([float(c[2]) for c in raw], dtype=float)
        highs   = np.array([float(c[3]) for c in raw], dtype=float)
        lows    = np.array([float(c[4]) for c in raw], dtype=float)
        volumes = np.array([float(c[1]) for c in raw], dtype=float)
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
    return 100.0 if al == 0 else round(100.0 - 100.0 / (1.0 + ag / al), 2)


def ema(closes: np.ndarray, period: int) -> float:
    if len(closes) < period:
        return float(closes[-1]) if len(closes) > 0 else 0.0
    k   = 2.0 / (period + 1)
    val = float(np.mean(closes[:period]))
    for p in closes[period:]:
        val = float(p) * k + val * (1 - k)
    return val


def ema_series(closes: np.ndarray, period: int) -> np.ndarray:
    """
    [FIX-1] Deret EMA lengkap — diperlukan macd() agar signal line benar.
    [FIX-D6] Index 0..period-2 diisi np.nan (bukan 0.0) agar kalkulasi
    downstream tidak salah menggunakan nilai pra-warmup sebagai data valid.
    """
    if len(closes) < period:
        return np.full(len(closes), closes[-1] if len(closes) > 0 else 0.0)
    k      = 2.0 / (period + 1)
    result = np.full(len(closes), np.nan)   # [FIX-D6] np.nan, bukan np.zeros
    result[period - 1] = float(np.mean(closes[:period]))
    for i in range(period, len(closes)):
        result[i] = float(closes[i]) * k + result[i - 1] * (1 - k)
    return result


def macd(closes: np.ndarray) -> tuple[float, float]:
    """
    [BUG-1 FIX] v2.2 memotong ema12 dan ema26 dari index 25 yang sama,
    menyebabkan phase mismatch — EMA 26 baru valid di index 25, EMA 12
    sudah valid di index 11, sehingga selisih di awal array tidak bermakna.
    v2.3: hitung full series dulu, ambil selisih, BARU potong dari index 25
    sehingga kedua EMA sudah fully warmed-up sebelum dikurangkan.
    """
    if len(closes) < 35:
        return 0.0, 0.0
    ema12_full = ema_series(closes, 12)
    ema26_full = ema_series(closes, 26)
    # Potong dari index 25 agar EMA 26 sudah fully warmed-up
    macd_line_series = (ema12_full - ema26_full)[25:]
    if len(macd_line_series) < 9:
        return (float(macd_line_series[-1]) if len(macd_line_series) > 0 else 0.0), 0.0
    signal_series = ema_series(macd_line_series, 9)
    return float(macd_line_series[-1]), float(signal_series[-1])


def atr(closes: np.ndarray, highs: np.ndarray, lows: np.ndarray, period: int = 14) -> float:
    """
    [KRITIS-1] v2.7 menghitung dari range(1, period+1) — yaitu index 0..14,
    candle TERTUA dalam window. Gate.io mengembalikan candle ascending (oldest first),
    sehingga ATR v2.7 mencerminkan volatilitas 28–56 hari lalu (masa flat dormancy),
    bukan volatilitas terkini saat breakout.

    v2.8: gunakan 14 candle TERBARU ([-period-1:-1] untuk prev close, [-period:] untuk
    high/low/close terkini) — ATR sekarang mencerminkan volatilitas aktual saat ini.
    """
    if len(closes) < period + 1:
        return 0.0
    # Ambil slice terbaru: index [-period-1:] agar prev_close tersedia
    c_slice = closes[-(period + 1):]
    h_slice = highs[-(period + 1):]
    l_slice = lows[-(period + 1):]
    trs = [
        max(h_slice[i] - l_slice[i],
            abs(h_slice[i] - c_slice[i - 1]),
            abs(l_slice[i] - c_slice[i - 1]))
        for i in range(1, len(c_slice))
    ]
    return float(np.mean(trs)) if trs else 0.0


def linear_slope(values: np.ndarray) -> float:
    """[FIX-5] Slope regresi linier dinormalisasi (% per candle)."""
    n    = len(values)
    mean = float(np.mean(values))
    if n < 3 or mean == 0:
        return 0.0
    slope = float(np.polyfit(np.arange(n, dtype=float), values, 1)[0])
    return (slope / mean) * 100


def detect_liquidity_sweep(closes: np.ndarray, highs: np.ndarray, lows: np.ndarray,
                            lookback: int = 72) -> bool:
    """
    [BUG-2 FIX] v2.2 menggunakan np.min() sebagai support level. Jika ada
    satu wick ekstrem di periode lookback, support menjadi sangat rendah
    sehingga recent_low tidak pernah bisa melewatinya — sweep tidak terdeteksi.
    v2.3: gunakan percentile ke-10 sebagai support yang lebih robust.
    [FIX-D3] v2.3 lookback 30 candle (5 hari) terlalu pendek vs flat window 60 candle.
    v2.4: lookback dinaikkan ke 72 candle (12 hari) — lebih representatif.
    """
    if len(closes) < lookback + 5:
        return False
    support    = float(np.percentile(lows[-(lookback + 5):-5], 10))  # bottom 10%, bukan absolute min
    recent_low = float(np.min(lows[-5:]))
    return (recent_low < support) and (float(closes[-1]) > support)


# ══════════════════════════════════════════════════════════════════
#  SESSION DETECTION [P8-SESS]
# ══════════════════════════════════════════════════════════════════

def is_prime_session() -> bool:
    """
    [P8-SESS] Return True jika jam WIB sekarang masuk window prime time Asia.
    Digunakan untuk relaksasi GEM_VOL_SPIKE_MIN → SESSION_VOL_SPIKE_RELAXED.
    Prime hours: 08:00–11:00 dan 20:00–23:00 WIB.
    """
    hour = datetime.now(WIB).hour
    return any(start <= hour < end for start, end in SESSION_PRIME_HOURS)


def get_vol_spike_threshold() -> float:
    """
    [P8-SESS] Return threshold vol spike yang sesuai dengan sesi saat ini.
    Prime time → lebih sensitif (2.5×), normal → standar (3.0×).
    """
    return SESSION_VOL_SPIKE_RELAXED if is_prime_session() else GEM_VOL_SPIKE_MIN


def get_scan_sleep() -> float:
    """
    [D5-SCAN] Return sleep antar ticker sesuai sesi saat ini.
    Prime time → 0.20s (lebih cepat), off-peak → SCAN_SLEEP_SEC (0.40s).
    Scan cycle prime time turun dari ~13 menit ke ~6.5 menit.
    """
    return SESSION_SCAN_SLEEP_SEC if is_prime_session() else SCAN_SLEEP_SEC


# ══════════════════════════════════════════════════════════════════
#  90-DAY HIGH DETECTION — MULTI-BATCH [P3-HIGH, FIX-2, A2]
# ══════════════════════════════════════════════════════════════════

GEM_HIGH90D_FETCH_BATCHES = 4   # [P3-HIGH] 4 × 1000 candle × 15m ≈ 40 hari coverage

def get_high_90d(client: SpotApi, pair: str, fallback: float) -> float:
    """
    [P3-HIGH] Multi-batch fetch untuk mendekati 40 hari coverage (bukan 10 hari).
    Gate.io limit=1000 per request, jadi fetch 4 batch dengan offset waktu.
    4 × 1000 × 15m = 40.000 menit ≈ 27.7 hari (mendekati 30 hari).

    [A2] PENTING — Ini BUKAN All-Time High (ATH) sejati.
    Fungsi ini mengembalikan harga tertinggi dalam ~40 hari terakhir.
    Untuk koin yang ATH-nya terjadi pada bull run 2021, nilai ini jauh lebih rendah
    dari ATH sejati. Jangan gunakan untuk klaim "jarak ke ATH" — gunakan hanya sebagai
    "ruang ke puncak observable periode terkini".

    Sebelumnya mislabeled sebagai get_ath_price(). Di-rename di v2.9 untuk kejelasan.
    Multi-batch ditambahkan di v3.2 [P3-HIGH] agar coverage mendekati 30 hari.
    """
    try:
        all_highs: list[float] = []
        # Fetch dari yang paling terkini mundur ke belakang menggunakan parameter `from`
        # Gate.io list_candlesticks mendukung parameter `_from` (Unix timestamp)
        now_ts = int(time.time())
        candle_sec = 15 * 60   # 15 menit dalam detik

        for batch in range(GEM_HIGH90D_FETCH_BATCHES):
            # Hitung timestamp akhir batch ini (mundur dari sekarang)
            to_ts   = now_ts - batch * 1000 * candle_sec
            from_ts = to_ts - 1000 * candle_sec

            try:
                raw = gate_retry(
                    client.list_candlesticks,
                    currency_pair=pair,
                    interval=GEM_TF,
                    limit=1000,
                    _from=from_ts,
                    to=to_ts,
                )
            except Exception as batch_err:
                # [C1-HIGH] Jika satu batch gagal, jangan buang data yang sudah ada.
                # Log error dan break — gunakan all_highs yang sudah terkumpul.
                log(f"get_high_90d batch {batch} [{pair}]: {batch_err} — pakai data parsial", "warn")
                break

            if raw and len(raw) >= 10:
                batch_highs = [float(c[3]) for c in raw]
                all_highs.extend(batch_highs)
            else:
                # Jika satu batch kosong, stop — tidak ada data lebih lama
                break

        # [C1-HIGH] Cek all_highs sebelum memutuskan fallback.
        # Sebelumnya: exception langsung ke fallback, all_highs yang sudah terisi dibuang.
        # Sekarang: jika sudah ada data valid (>= 96 candle = 1 hari), pakai itu.
        if len(all_highs) >= 96:
            return float(max(all_highs))

    except Exception as e:
        log(f"get_high_90d multi-batch [{pair}]: {e} — fallback ke single fetch", "warn")

    # Fallback ke single fetch jika multi-batch tidak menghasilkan data cukup
    try:
        raw = gate_retry(client.list_candlesticks, currency_pair=pair,
                         interval=GEM_TF, limit=GEM_HIGH90D_CANDLE_LIMIT)
        if raw and len(raw) >= 96:
            return float(max(float(c[3]) for c in raw))
    except Exception as e2:
        log(f"get_high_90d fallback [{pair}]: {e2}", "warn")

    return fallback


# ══════════════════════════════════════════════════════════════════
#  DYNAMIC TP [FIX-B5]
# ══════════════════════════════════════════════════════════════════

def calc_dynamic_tp(entry: float, high90d_dist: float, high90d_price: float) -> tuple[float, float, float]:
    """
    [FIX-B5] TP berbasis ruang ke 90d high, bukan fixed percentage.
    Koin 95% di bawah 90d high punya ruang yang jauh lebih besar dari koin 35% di bawah.

    [A2] TP3 untuk bracket high90d_dist ≥ 0.85 sebelumnya adalah +2000% fixed.
    Ini tidak realistis karena 90d high bukan ATH sejati — koin bisa saja sudah
    pulih sebagian dari ATH 2021-nya. TP3 sekarang dikap pada entry × (high90d_price/entry) × 1.5,
    yaitu 50% di atas puncak 90 hari yang observable. Ini ambisius tapi masih
    terikat pada data harga historis yang nyata.

    [FIX-D7] TP3 disesuaikan per bracket:
      - high90d_dist ≥ 0.85: TP3 = min(+2000%, entry × (high90d / entry) × 1.5)
      - high90d_dist ≥ 0.70: TP3 = +900%
      - high90d_dist ≥ 0.50: TP3 = +400%
      - high90d_dist  < 0.50: TP3 = +150%
    """
    if high90d_dist >= 0.85:
        tp1_pct, tp2_pct = 0.40, 1.50
        # [A2] Kap TP3 pada 50% di atas 90d high — mencegah target +2000% yang
        # disconnect dari realita jika 90d high sendiri sudah rendah
        tp3_raw    = entry * (1 + 20.00)   # batas atas teoritis +2000%
        tp3_capped = entry * (high90d_price / entry) * 1.5 if high90d_price > entry else tp3_raw
        tp3_pct    = (min(tp3_raw, tp3_capped) - entry) / entry
    elif high90d_dist >= 0.70:
        tp1_pct, tp2_pct, tp3_pct = 0.40, 1.20, 9.00
    elif high90d_dist >= 0.50:
        tp1_pct, tp2_pct, tp3_pct = 0.35, 1.00, 4.00
    else:
        tp1_pct, tp2_pct, tp3_pct = 0.25, 0.70, 1.50

    # [D4-TP] Redundant if/else dihapus — kedua branch sebelumnya identik:
    #   tp3 = round(entry * (1 + tp3_pct), 8)  regardless of bracket.
    tp3 = round(entry * (1 + tp3_pct), 8)

    return (
        round(entry * (1 + tp1_pct), 8),
        round(entry * (1 + tp2_pct), 8),
        tp3,
    )


# ══════════════════════════════════════════════════════════════════
#  MULTI-TIMEFRAME CONFIRMATION + TF ALIGNMENT [FIX-B3 + UPG-3]
# ══════════════════════════════════════════════════════════════════

def check_mtf(client: SpotApi, pair: str, closes_15m: np.ndarray | None = None) -> dict:
    """
    [F1-SPEED] MTF sekarang menggunakan timeframe 5m sebagai konfirmasi cepat
    (menggantikan 1h), dan 1h sebagai konfirmasi medium (menggantikan 1d).
    Tujuannya mempercepat sinyal agar tidak ketinggalan pump instan.

    [UPG-3] TF alignment score sekarang dihitung dalam check_mtf
    menggunakan data yang sudah di-fetch — zero extra API calls.

    [P2-MTF] Field naming dikoreksi di v3.2:
      rsi_fast = RSI dari TF konfirmasi cepat (5m)
      rsi_mid  = RSI dari TF konfirmasi medium (1h)
    Sebelumnya field bernama rsi_1h (isi RSI 5m) dan rsi_1d (isi RSI 1h)
    yang sangat menyesatkan saat debugging.

    [D1-MTF] Parameter di-rename closes_4h → closes_15m (v3.4).
    Sebelumnya mislabeled — yang dikirim adalah closes array dari
    fetch_candles() dengan GEM_TF = 15m, bukan 4h data.
    """
    result = {
        "ok": True, "rsi_fast": 50.0, "rsi_mid": 50.0,
        "trend_1h": True, "mtf_bonus": 0, "tf_alignment": 0, "detail": [],
    }
    if not MTF_ENABLED:
        return result

    # TF alignment: 15m (dari closes yang sudah di-fetch di analyze_gem)
    if closes_15m is not None and len(closes_15m) >= 20:
        if closes_15m[-1] > ema(closes_15m, 20) and rsi(closes_15m) > 45:
            result["tf_alignment"] += 1

    # [F1-SPEED] Track apakah masing-masing TF berhasil di-fetch.
    # Jika fetch gagal, koin tidak boleh lolos MTF secara default.
    fetched_fast = False   # 5m (konfirmasi cepat)
    fetched_mid  = False   # 1h (konfirmasi medium)

    try:
        # [F1-SPEED] Konfirmasi cepat: 5m (ganti dari 1h)
        raw_5m = gate_retry(client.list_candlesticks, currency_pair=pair, interval=GEM_TF_CONFIRM, limit=50)
        if raw_5m and len(raw_5m) >= 15:
            fetched_fast   = True
            closes_5m      = np.array([float(c[2]) for c in raw_5m], dtype=float)
            rsi_5m         = rsi(closes_5m)
            result["rsi_fast"] = rsi_5m   # [P2-MTF] rsi_fast = RSI 5m (konfirmasi cepat)
            if rsi_5m > MTF_RSI_FAST_MAX:   # [C4-MTF] renamed from MTF_RSI_1H_MAX
                result["ok"] = False
                result["detail"].append(f"RSI 5m overbought ({rsi_5m:.0f})")
            elif rsi_5m < 55:
                result["mtf_bonus"] += 1
            # TF alignment: 5m
            if closes_5m[-1] > ema(closes_5m, 20) and rsi_5m > 45:
                result["tf_alignment"] += 1

        # [F1-SPEED] Konfirmasi medium: 1h (ganti dari 1d)
        raw_1h = gate_retry(client.list_candlesticks, currency_pair=pair, interval="1h", limit=30)
        if raw_1h and len(raw_1h) >= 10:
            fetched_mid    = True
            closes_1h      = np.array([float(c[2]) for c in raw_1h], dtype=float)
            rsi_1h         = rsi(closes_1h)
            price_now      = float(closes_1h[-1])
            result["rsi_mid"]   = rsi_1h   # [P2-MTF] rsi_mid = RSI 1h (konfirmasi medium)
            result["trend_1h"] = price_now > ema(closes_1h, 20)
            if not (MTF_RSI_MID_MIN <= rsi_1h <= MTF_RSI_MID_MAX):   # [C4-MTF] renamed from MTF_RSI_1D_*
                result["ok"] = False
                result["detail"].append(f"RSI 1h di luar range ({rsi_1h:.0f})")
            else:
                result["mtf_bonus"] += 1
            if result["trend_1h"]:
                result["mtf_bonus"] += 1
            # TF alignment: 1h
            if result["trend_1h"] and rsi_1h > 45:
                result["tf_alignment"] += 1

    except Exception as e:
        log(f"MTF [{pair}]: {e}", "warn")

    # Jika salah satu TF gagal di-fetch, tolak koin
    if not fetched_fast or not fetched_mid:
        result["ok"] = False
        missing = []
        if not fetched_fast: missing.append("5m")
        if not fetched_mid:  missing.append("1h")
        result["detail"].append(f"MTF data tidak tersedia: {', '.join(missing)}")
        log(f"  MTF [{pair}] fetch gagal ({', '.join(missing)}) — koin ditolak", "warn")

    return result


# ══════════════════════════════════════════════════════════════════
#  ORDER BOOK DEPTH [UPG-1]
# ══════════════════════════════════════════════════════════════════

def check_orderbook_depth(client: SpotApi, pair: str) -> dict:
    """
    [UPG-1] Analisis kedalaman order book — bid wall adalah sinyal akumulasi
    smart money yang tidak terdeteksi oleh indikator price/volume biasa.
    """
    result = {"ok": True, "spread_pct": 0.0, "depth_ratio": 1.0, "depth_score": 0}
    if not LIQ_ENABLED:
        return result
    try:
        ob = gate_retry(client.list_order_book, currency_pair=pair, limit=OB_DEPTH_LEVELS)
        if not ob or not ob.bids or not ob.asks:
            return result

        bid0 = float(ob.bids[0][0])
        ask0 = float(ob.asks[0][0])
        if bid0 <= 0:
            return result

        spread = (ask0 - bid0) / bid0 * 100
        result["spread_pct"] = round(spread, 2)
        if spread > LIQ_SPREAD_MAX_PCT:
            result["ok"] = False
            return result

        mid = (bid0 + ask0) / 2.0
        lower = mid * (1 - OB_DEPTH_WINDOW_PCT)
        upper = mid * (1 + OB_DEPTH_WINDOW_PCT)

        # [FIX-D1] Filter bid DAN ask dengan window ketat: lower ≤ price ≤ mid/upper
        # v2.3 hanya filter satu sisi → jika Gate.io kirim crossed-book data,
        # asks di bawah mid ikut terhitung → depth_ratio terinflasi palsu.
        bid_depth = sum(
            float(b[0]) * float(b[1])
            for b in ob.bids
            if lower <= float(b[0]) <= mid
        )
        ask_depth = sum(
            float(a[0]) * float(a[1])
            for a in ob.asks
            if mid <= float(a[0]) <= upper
        )

        if ask_depth > 0:
            depth_ratio = bid_depth / ask_depth
        else:
            depth_ratio = 1.0

        result["depth_ratio"] = round(depth_ratio, 2)
        if depth_ratio >= OB_DEPTH_RATIO_HIGH:
            result["depth_score"] = 2
        elif depth_ratio >= OB_DEPTH_RATIO_MED:
            result["depth_score"] = 1

    except Exception as e:
        log(f"OB depth [{pair}]: {e}", "warn")
    return result


# ══════════════════════════════════════════════════════════════════
#  S1: TRADE FREQUENCY FILTER [S1-FREQ]
# ══════════════════════════════════════════════════════════════════

def check_trade_frequency(client: SpotApi, pair: str) -> dict:
    """
    [S1-FREQ] Deteksi lonjakan frekuensi transaksi — sinyal paling awal
    bahwa ada aktivitas bot dari exchange lain yang bocor ke Gate.io.

    Koin yang "tidur" biasanya cuma punya 1–3 transaksi per menit.
    Jika tiba-tiba ada 15–30+ transaksi kecil per menit, itu tanda
    bot akumulasi dari Indodax/Tokocrypto sudah mulai bekerja di
    sini juga, sebelum harganya sempat naik.

    Gate.io API list_trades() mengembalikan recent trades dengan
    timestamp Unix. Kita hitung berapa yang terjadi dalam 60 detik
    terakhir untuk mendapat tx/menit yang akurat.

    Return dict:
      tx_per_min  : jumlah transaksi per menit (float)
      freq_spike  : True jika ≥ TRADE_FREQ_SPIKE_MIN
      freq_strong : True jika ≥ TRADE_FREQ_STRONG (bot aktif)
      freq_score  : 0, 1, atau 2 untuk scoring
    """
    result = {"tx_per_min": 0.0, "freq_spike": False, "freq_strong": False, "freq_score": 0}
    if not TRADE_FREQ_ENABLED:
        return result
    try:
        trades = gate_retry(
            client.list_trades,
            currency_pair=pair,
            limit=TRADE_FREQ_FETCH_LIMIT,
        )
        if not trades:
            return result

        now_ts    = time.time()
        cutoff_ts = now_ts - TRADE_FREQ_WINDOW_SEC

        # Hitung transaksi dalam window 60 detik terakhir
        recent_count = 0
        for tr in trades:
            try:
                # Gate.io trade timestamp bisa berupa string detik atau milidetik
                ts_raw = getattr(tr, "create_time", None) or getattr(tr, "time", None)
                if ts_raw is None:
                    continue
                ts = float(ts_raw)
                # Normalisasi: jika timestamp dalam milidetik (> 1e12), bagi 1000
                if ts > 1e12:
                    ts /= 1000.0
                if ts >= cutoff_ts:
                    recent_count += 1
            except Exception:
                continue

        # Konversi ke tx/menit (window = TRADE_FREQ_WINDOW_SEC detik)
        tx_per_min = recent_count * (60.0 / TRADE_FREQ_WINDOW_SEC)
        result["tx_per_min"]  = round(tx_per_min, 1)
        result["freq_spike"]  = tx_per_min >= TRADE_FREQ_SPIKE_MIN
        result["freq_strong"] = tx_per_min >= TRADE_FREQ_STRONG

        if result["freq_strong"]:
            result["freq_score"] = 2
        elif result["freq_spike"]:
            result["freq_score"] = 1

    except Exception as e:
        log(f"trade_freq [{pair}]: {e}", "warn")
    return result


# ══════════════════════════════════════════════════════════════════
#  S2: MICRO MARKET CAP ESTIMATOR [S2-MCAP]
# ══════════════════════════════════════════════════════════════════

def estimate_micro_mcap(price: float, vol_24h: float, sector: str = "—") -> dict:
    """
    [S2-MCAP] Estimasi market cap untuk identifikasi koin "micro" yang
    masih punya ruang 50x–100x. Koin besar seperti SOL/AVAX tidak akan
    pernah bisa 100x lagi — capitalnya sudah terlalu besar.

    Metode estimasi:
    MCap ≈ vol_24h / turnover_rate
    Turnover rate default 10% (asumsi 10% MCap diperdagangkan/hari).
    Sektor high-velocity (MEME, AI_AGENT) punya turnover lebih tinggi.

    Label tambahan:
    - "ULTRA GEM 💎💎" jika est. MCap < $5 Juta DAN harga < $0.10
    - "MICRO GEM" jika est. MCap < $20 Juta

    Return dict:
      est_mcap_usd  : estimasi market cap dalam USD
      mcap_label    : string label untuk display
      is_ultra_gem  : True jika memenuhi kriteria ULTRA GEM
      is_micro      : True jika est. MCap < MCAP_GEM_USD
      mcap_score    : 0 atau 1 (bonus score untuk ultra gem)
    """
    HIGH_TURNOVER = {"MEME", "SOLANA_ECO", "AI_AGENT"}
    LOW_TURNOVER  = {"DEFI", "INFRA", "PRIVACY", "RWA", "RESTAKE"}

    if sector in HIGH_TURNOVER:
        turnover = 0.25   # meme coin berputar cepat
    elif sector in LOW_TURNOVER:
        turnover = 0.05   # DeFi/infra lebih lambat
    else:
        turnover = MCAP_TURNOVER_ASSUMED

    if turnover <= 0 or vol_24h <= 0:
        return {"est_mcap_usd": 0, "mcap_label": "UNKNOWN", "is_ultra_gem": False,
                "is_micro": False, "mcap_score": 0}

    est_mcap = vol_24h / turnover

    is_ultra_gem = (est_mcap < MCAP_ULTRA_GEM_USD) and (price < MCAP_PRICE_MAX)
    is_micro     = est_mcap < MCAP_GEM_USD

    if is_ultra_gem:
        mcap_label = f"ULTRA GEM 💎💎 (~${est_mcap/1_000_000:.2f}M)"
        mcap_score = 1
    elif is_micro:
        mcap_label = f"MICRO GEM (~${est_mcap/1_000_000:.1f}M)"
        mcap_score = 0
    elif est_mcap < 100_000_000:
        mcap_label = f"SMALL (~${est_mcap/1_000_000:.0f}M)"
        mcap_score = 0
    else:
        mcap_label = f"MID/LARGE (~${est_mcap/1_000_000:.0f}M)"
        mcap_score = 0

    return {
        "est_mcap_usd": round(est_mcap, 0),
        "mcap_label":   mcap_label,
        "is_ultra_gem": is_ultra_gem,
        "is_micro":     is_micro,
        "mcap_score":   mcap_score,
    }


# ══════════════════════════════════════════════════════════════════
#  S3: VOLUME WALL CONSISTENCY CHECK [S3-WALL]
# ══════════════════════════════════════════════════════════════════

def check_volume_wall(client: SpotApi, pair: str, candle_vol_median: float) -> dict:
    """
    [S3-WALL] Cross-validate antara volume spike breakout dan ketebalan
    Buy Wall di order book — membedakan breakout LEGIT vs FAKE PUMP.

    Logika:
    Wash trading (jual beli sendiri) menghasilkan volume besar tapi
    tidak meninggalkan jejak buy wall di order book — pelaku bisa
    hapus order setelah dieksekusi sendiri.

    Breakout legit sebaliknya meninggalkan bid wall tebal di bawah
    harga: smart money akumulasi sambil memasang support agar harga
    tidak jatuh setelah mereka mendorong naik.

    Check:
    1. Hitung total bid depth dalam window 2% di bawah mid price
    2. Bandingkan dengan median candle volume (likuiditas koin)
    3. Jika bid wall ≥ OB_WALL_VOL_RATIO × median_vol → LEGIT
    4. Jika volume spike besar tapi wall tipis → kemungkinan fake

    Return dict:
      wall_usdt      : total bid depth dalam window 2% (USDT)
      wall_ratio     : wall_usdt / candle_vol_median
      wall_legit     : True jika wall mencukupi vs volume
      wall_score     : 0, 1, atau 2
      fake_pump_warn : True jika vol spike tapi wall sangat tipis
    """
    result = {
        "wall_usdt": 0.0, "wall_ratio": 0.0,
        "wall_legit": False, "wall_score": 0, "fake_pump_warn": False,
    }
    try:
        ob = gate_retry(client.list_order_book, currency_pair=pair, limit=50)
        if not ob or not ob.bids or not ob.asks:
            return result

        bid0 = float(ob.bids[0][0])
        ask0 = float(ob.asks[0][0])
        if bid0 <= 0:
            return result

        mid    = (bid0 + ask0) / 2.0
        floor  = mid * (1 - OB_WALL_WINDOW_PCT)   # 2% di bawah mid

        # Hitung total nilai bid dalam window [floor, mid]
        wall_usdt = sum(
            float(b[0]) * float(b[1])
            for b in ob.bids
            if floor <= float(b[0]) <= mid
        )
        result["wall_usdt"] = round(wall_usdt, 2)

        # [P7-WALL] Dynamic minimum wall: max($500, candle_vol_median × 0.3)
        # Wall flat $1000 tidak bermakna untuk koin sangat sepi (vol median $200/candle)
        # maupun terlalu rendah untuk koin vol medium ($2000/candle)
        dynamic_wall_min = max(500.0, candle_vol_median * 0.3) if candle_vol_median > 0 else OB_WALL_MIN_USDT

        # Bandingkan dengan median candle volume sebagai baseline likuiditas
        if candle_vol_median > 0:
            wall_ratio = wall_usdt / candle_vol_median
            result["wall_ratio"] = round(wall_ratio, 3)

            result["wall_legit"] = (
                wall_usdt >= dynamic_wall_min and
                wall_ratio >= OB_WALL_VOL_RATIO
            )

            if result["wall_legit"]:
                result["wall_score"] = 2 if wall_ratio >= OB_WALL_VOL_RATIO * 3 else 1
            elif wall_usdt < dynamic_wall_min * 0.3:
                # Wall sangat tipis vs baseline dinamis — kemungkinan fake pump / wash trading
                result["fake_pump_warn"] = True

    except Exception as e:
        log(f"vol_wall [{pair}]: {e}", "warn")
    return result

def get_sector_momentum(pair: str, ticker_map: dict[str, float]) -> dict:
    """
    [UPG-2] Sektor rotation detection menggunakan ticker_map yang sudah
    di-cache di awal scan — zero additional API calls.
    Koin dalam sektor yang sedang hot mendapat bonus signifikan.
    """
    base = pair.replace("_USDT", "")
    active_sector: str | None = None
    for sector, coins in SECTOR_MAP.items():
        if base in coins:
            active_sector = sector
            break

    if not active_sector:
        return {"sector": "—", "sector_hot": False, "sector_bonus": 0, "pumped_count": 0}

    pumped = 0
    observed = 0
    for coin in SECTOR_MAP[active_sector]:
        if coin == base:
            continue
        chg = ticker_map.get(f"{coin}_USDT")
        if chg is not None:
            observed += 1
            if chg > SECTOR_HOT_THRESHOLD:
                pumped += 1

    sector_hot = pumped >= SECTOR_HOT_MIN_COUNT and observed >= 3
    return {
        "sector": active_sector,
        "sector_hot": sector_hot,
        "pumped_count": pumped,
        "sector_bonus": 2 if sector_hot else 0,
    }


# ══════════════════════════════════════════════════════════════════
#  MARKET CAP TIER ESTIMATOR [UPG-4]
# ══════════════════════════════════════════════════════════════════

def estimate_mcap_tier(vol_24h: float, sector: str = "—") -> str:
    """
    [WEAK-4 FIX] v2.2 menggunakan asumsi turnover 5–20% flat untuk semua koin.
    v2.3: turnover rate disesuaikan per kategori koin:
    - MEME: turnover sangat tinggi (20–100%)
    - DEFI/INFRA: turnover moderate (5–15%)
    - Lainnya: default 5–20%
    Display only — tidak mempengaruhi scoring.
    """
    HIGH_TURNOVER_SECTORS = {"MEME", "SOLANA_ECO", "AI_AGENT"}
    LOW_TURNOVER_SECTORS  = {"DEFI", "INFRA", "PRIVACY", "RWA", "RESTAKE"}

    if sector in HIGH_TURNOVER_SECTORS:
        est_low  = vol_24h / 1.00   # turnover ~100%
        est_high = vol_24h / 0.20   # turnover ~20%
    elif sector in LOW_TURNOVER_SECTORS:
        est_low  = vol_24h / 0.15   # turnover ~15%
        est_high = vol_24h / 0.03   # turnover ~3%
    else:
        est_low  = vol_24h / 0.20
        est_high = vol_24h / 0.05

    if est_high < 100_000:
        return "MICRO (&lt;$100K)"
    if est_low < 2_000_000:
        return "NANO ($100K–$2M)"
    if est_low < 20_000_000:
        return "SMALL ($2M–$20M)"
    return "MID (>$20M)"


# ══════════════════════════════════════════════════════════════════
#  BTC MARKET REGIME [FIX-B3]
# ══════════════════════════════════════════════════════════════════

def get_btc_regime(client: SpotApi) -> dict:
    """
    [F4-BTC] Bot tidak lagi halt saat BTC sideways atau bearish ringan.
    Koin micin sering pump justru saat BTC membosankan — memblokir bot
    saat BTC sideways menyebabkan banyak sinyal bagus terlewat.

    Kondisi halt sekarang hanya untuk crash parah: BTC 4h turun >8%.
    block_buy (threshold ketat) hanya aktif saat BTC 4h turun >5% —
    sebelumnya -4%, terlalu mudah trigger saat koreksi normal.

    [A5] Fail-safe tetap dipertahankan: jika data BTC tidak tersedia,
    return halt=True — lebih aman diam daripada scan tanpa info.
    """
    result = {"btc_1h": 0.0, "btc_4h": 0.0, "halt": False, "block_buy": False}
    try:
        # 1h: bandingkan close terkini vs 6 candle lalu (6 jam lalu)
        c1h = gate_retry(client.list_candlesticks, currency_pair="BTC_USDT",
                         interval="1h", limit=8)
        if c1h and len(c1h) >= 7:
            prev = float(c1h[-7][2])
            cur  = float(c1h[-1][2])
            result["btc_1h"] = (cur - prev) / prev * 100 if prev > 0 else 0.0
        else:
            log("BTC data 1h tidak tersedia — scan dihalt sebagai precaution", "warn")
            return {"btc_1h": 0.0, "btc_4h": 0.0, "halt": True, "block_buy": True}

        # 4h: bandingkan close terkini vs 4 candle lalu (16 jam lalu)
        c4h = gate_retry(client.list_candlesticks, currency_pair="BTC_USDT",
                         interval="4h", limit=6)
        if c4h and len(c4h) >= 5:
            prev = float(c4h[-5][2])
            cur  = float(c4h[-1][2])
            result["btc_4h"] = (cur - prev) / prev * 100 if prev > 0 else 0.0
        else:
            log("BTC data 4h tidak tersedia — scan dihalt sebagai precaution", "warn")
            return {"btc_1h": result["btc_1h"], "btc_4h": 0.0, "halt": True, "block_buy": True}

        # [F4-BTC] halt hanya saat crash parah; block_buy hanya saat drop signifikan
        result["halt"]      = result["btc_4h"] <= BTC_HALT_PCT    # -8% 4h
        result["block_buy"] = result["btc_4h"] <= BTC_BLOCK_PCT   # [F4-BTC] -5% 4h (dari -4%)
    except Exception as e:
        log(f"btc_regime exception: {e} — scan dihalt sebagai precaution", "warn")
        return {"btc_1h": 0.0, "btc_4h": 0.0, "halt": True, "block_buy": True}
    return result


# ══════════════════════════════════════════════════════════════════
#  IDR CONVERSION
# ══════════════════════════════════════════════════════════════════

_idr_cache: dict = {}

def get_usdt_idr() -> float:
    global _idr_cache
    now = time.time()
    if _idr_cache.get("ts", 0) + 1800 > now:
        return _idr_cache.get("rate", 15500.0)
    try:
        r    = requests.get("https://api.coingecko.com/api/v3/simple/price",
                            params={"ids": "tether", "vs_currencies": "idr"}, timeout=10)
        rate = float(r.json()["tether"]["idr"])
        if rate <= 0:
            raise ValueError(f"CoinGecko IDR rate invalid: {rate}")
        _idr_cache = {"rate": rate, "ts": now}
        return rate
    except Exception:
        return _idr_cache.get("rate", 15500.0)


def fmt_idr(usdt: float) -> str:
    idr = usdt * get_usdt_idr()
    if idr >= 1_000_000_000: return f"Rp {idr/1_000_000_000:.2f}M"
    if idr >= 1_000_000:     return f"Rp {idr/1_000_000:.2f}jt"
    if idr >= 1_000:         return f"Rp {idr:,.0f}"
    return f"Rp {idr:.2f}"


# ══════════════════════════════════════════════════════════════════
#  SUPABASE
# ══════════════════════════════════════════════════════════════════

_sb: SupabaseClient | None = None

def sb() -> SupabaseClient:
    global _sb
    if _sb is None:
        _sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _sb


def load_dedup_cache() -> set[str]:
    """
    [FIX-B4] Load semua pair yang sudah dikirim dalam window GEM_DEDUP_HOURS
    SEKALI di awal scan — menggantikan query per-pair yang hammers Supabase
    dengan ratusan request individu.

    [MEDIUM-5] Jika mark_sent() gagal setelah save_signal() berhasil (non-atomic),
    pair tidak masuk gem_dedup sehingga run berikutnya bisa kirim duplikat.
    Mitigasi: load juga pair dari gem_signals dalam window yang sama — sinyal yang
    sudah tersimpan di gem_signals tidak akan dikirim ulang meski gem_dedup kosong.
    Union kedua set memastikan keduanya saling backup.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=GEM_DEDUP_HOURS)).isoformat()
    cache: set[str] = set()
    try:
        res = sb().table("gem_dedup").select("pair").gte("sent_at", cutoff).execute()
        cache.update(r["pair"] for r in (res.data or []))
        log(f"Dedup cache dari gem_dedup: {len(cache)} pairs")
    except Exception as e:
        log(f"load_dedup_cache (gem_dedup): {e}", "warn")
    try:
        # [MEDIUM-5] Safety net: pair yang ada di gem_signals juga masuk cache
        res2 = sb().table("gem_signals").select("pair").gte("created_at", cutoff).execute()
        before = len(cache)
        cache.update(r["pair"] for r in (res2.data or []))
        added = len(cache) - before
        if added:
            log(f"Dedup cache dari gem_signals: +{added} pairs (safety net)")
    except Exception as e:
        log(f"load_dedup_cache (gem_signals): {e}", "warn")
    log(f"Dedup cache total: {len(cache)} pairs")
    return cache


def mark_sent(pair: str) -> None:
    try:
        sb().table("gem_dedup").insert({
            "pair": pair, "sent_at": datetime.now(timezone.utc).isoformat()
        }).execute()
    except Exception as e:
        log(f"mark_sent [{pair}]: {e}", "warn")


def save_signal(sig: dict) -> None:
    """
    [FIX-B2] high_watermark diinisialisasi dari entry price.
    [MEDIUM-6] flat_vol_median disimpan ke DB — digunakan check_outcomes() untuk
    menghitung signal_vol secara akurat tanpa harus re-estimasi dari candle terkini.
    """
    try:
        sb().table("gem_signals").insert({
            "pair": sig["pair"], "tier": sig["tier"], "score": sig["score"],
            "entry": sig["entry"], "tp1": sig["tp1"], "tp2": sig["tp2"], "tp3": sig["tp3"],
            "sl": sig["sl"], "rr": sig["rr"], "rsi": sig["rsi"],
            "vol_ratio": sig["vol_ratio"], "breakout_pct": sig["breakout_pct"],
            "avg_flat_range": sig["avg_flat_range"],
            # [E3-ATH] ath_dist_pct removed — high90d_dist_pct is the canonical field
            "dist_from_base": sig["dist_from_base"], "change_24h": sig["change_24h"],
            "is_new_listing": sig["is_new_listing"], "has_sweep": sig["has_sweep"],
            "macd_bull": sig["macd_bull"], "atr_pct": sig["atr_pct"],
            "sl_atr_based": sig["sl_atr_based"],
            "high_watermark": sig["entry"],        # [FIX-B2] inisialisasi = entry
            "vol30_score": sig["vol30_score"],     # [UPG-C1]
            "narrative": sig["narrative"],          # [UPG-C2]
            "ultra_low_liq": sig["ultra_low_liq"], # [WEAK-1]
            "candle_vol_median": sig["candle_vol_median"],  # [ISU-1] median vol per candle 15m [D3-UNIT]
            "flat_vol_median": sig["flat_vol_median"],      # [MEDIUM-6] baseline vol flat period
            "flat_quality_ratio": sig.get("flat_quality_ratio"),  # [C5-SAVE] % candle dalam batas flat
            "vol_weak_alerted": False,             # [FIX-D8]
            "base_score":  sig.get("base_score"),   # [F1-SAVE] v3.6 — tier basis (max 32)
            "bonus_score": sig.get("bonus_score"),  # [F1-SAVE] v3.6 — S1+S2+S3 bonus (max 5)
            "created_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e:
        log(f"save_signal [{sig.get('pair')}]: {e}", "warn")


# ══════════════════════════════════════════════════════════════════
#  ANTI-PUMP FILTER
# ══════════════════════════════════════════════════════════════════

def is_manipulative_pump(closes: np.ndarray, volumes: np.ndarray,
                          vol_ratio: float, change_24h: float) -> bool:
    """[FIX-11] recent_vols dipakai — pump candle harus ada konfirmasi volume."""
    if vol_ratio > PUMP_VOL_SPIKE_MAX:
        log(f"  ⚠ vol spike {vol_ratio:.0f}x — manipulatif", "warn")
        return True
    if change_24h > PUMP_PRICE_SPIKE_MAX:
        log(f"  ⚠ naik {change_24h:.0f}% 24h — skip", "warn")
        return True
    # [FIX-D2] v2.3 pakai body/prev > 0.85 yang artinya harga harus naik 85%
    # dalam 1 candle — threshold ini tidak masuk akal untuk koin small cap yang
    # legitimately breakout 10–30%. Koreksi: gunakan PUMP_CANDLE_BODY_PCT (15%)
    # sebagai % kenaikan 1 candle, bukan rasio absolut terhadap harga.
    if len(closes) >= 6:
        recent_closes  = closes[-6:]
        recent_vols    = volumes[-6:]
        avg_vol_recent = float(np.mean(recent_vols))
        for i in range(1, len(recent_closes)):
            prev = recent_closes[i - 1]
            if prev <= 0:
                continue
            pct_move = abs(recent_closes[i] - prev) / prev * 100
            # [MINOR-8] Jika avg_vol_recent == 0 (koin baru listing, beberapa candle awal
            # masih zero volume), vol_confirm harusnya False — tidak ada konfirmasi volume
            # sama sekali, sehingga tidak bisa dianggap sebagai pump candle terkonfirmasi.
            # v2.7 pakai `True` sebagai fallback → setiap pump candle jadi "manipulatif".
            vol_confirm = (recent_vols[i] > avg_vol_recent * 2) if avg_vol_recent > 0 else False
            if pct_move > PUMP_CANDLE_BODY_PCT and vol_confirm:
                log(f"  ⚠ pump candle {pct_move:.0f}% + vol confirm — skip", "warn")
                return True
    return False


# ══════════════════════════════════════════════════════════════════
#  30-DAY VOLUME ANOMALY SCORE [UPG-C1]
# ══════════════════════════════════════════════════════════════════

def calc_vol30_score(
    current_vol_ratio: float,
    prefetched_volumes: np.ndarray | None = None,
) -> int:
    """
    [UPG-C1] Bandingkan volume breakout bukan hanya terhadap flat period,
    tapi juga terhadap rata-rata volume 30 hari. Koin yang volumenya
    50× di atas baseline 30-hari lebih powerful dari koin yang 8×
    di atas flat period yang kebetulan juga selalu sepi.
    Menambah +0, +1, atau +2 ke total score.

    [E4-VOL30] v3.5: else-branch dead code dihapus. GEM_CANDLE_LIMIT=384
    selalu >= 150 sehingga prefetch selalu cukup. Jika tidak tersedia,
    return 0 langsung — tidak ada separate API fetch.

    [G1-PARAM] v3.7: parameter 'client' dan 'pair' dihapus — tidak lagi
    digunakan setelah E4 menghapus API call di dalam fungsi ini.
    """
    try:
        # [E4-VOL30] Else-branch dead code dihapus. GEM_CANDLE_LIMIT=384 selalu >= 150,
        # sehingga prefetched_volumes selalu cukup. Jika tidak tersedia (None atau
        # kurang dari 150 candle), return 0 langsung — tidak ada separate API fetch.
        if prefetched_volumes is None or len(prefetched_volumes) < 150:
            return 0

        vols_30d    = prefetched_volumes
        baseline_30 = float(np.median(vols_30d[:-6]))   # median, kecuali 6 candle terakhir
        if baseline_30 <= 0:
            return 0
        recent_max = float(np.max(vols_30d[-6:]))
        ratio_30d  = recent_max / baseline_30
        if ratio_30d >= 50:
            return 2
        if ratio_30d >= 20:
            return 1
        return 0
    except Exception as e:
        log(f"vol30_score [{pair}]: {e}", "warn")
        return 0


# ══════════════════════════════════════════════════════════════════
#  NARRATIVE KEYWORD BONUS [UPG-C2]
# ══════════════════════════════════════════════════════════════════

def get_narrative_bonus(pair: str, sector: str) -> tuple[int, str]:
    """
    [UPG-C2] Bonus +1 jika base token mengandung keyword narasi aktif
    2025–2026, ATAU jika sektornya adalah AI_AGENT, DEPIN, BTCFI, RESTAKE.
    Narasi ini adalah driver terkuat untuk 50–100x moves saat ini.

    [FIX-E4] Prefix fallback DIHAPUS — terlalu banyak false positive.
    Contoh: "BOTANIC" match "BOT", "LIQUIDSWAP" match "LIQUID".
    Sekarang hanya exact-match dari NARRATIVE_KEYWORDS yang diterima.
    """
    HOT_SECTORS = {"AI_AGENT", "DEPIN", "BTCFI", "RESTAKE"}
    if sector in HOT_SECTORS:
        return 1, sector

    # [FIX-D5 + FIX-E4] Exact-match only — tidak ada prefix fallback
    base = pair.replace("_USDT", "").upper()
    if base in NARRATIVE_KEYWORDS:
        return 1, base

    return 0, "—"


# ══════════════════════════════════════════════════════════════════
#  CORE: GEM HUNTER SCANNER
#  [A9] analyze_gem() didekomposisi menjadi tiga fungsi terpisah:
#    1. run_gate_filters()  — evaluasi semua gate, return None jika gagal
#    2. compute_scores()    — hitung semua sub-score dari data yang lolos gate
#    3. build_signal()      — assembling dict sinyal final
#  Cyclomatic complexity turun dari ~15 ke ~4–5 per fungsi.
#  analyze_gem() kini hanya orchestrator tipis (~20 baris).
# ══════════════════════════════════════════════════════════════════

def run_gate_filters(
    client: SpotApi,
    pair: str, price: float, vol_24h: float, change_24h: float,
    closes: np.ndarray, highs: np.ndarray, lows: np.ndarray, volumes: np.ndarray,
    btc_block_buy: bool = False,
    gate_stats: dict | None = None,   # [DIAG] pass dict untuk kumpulkan statistik
) -> dict | None:
    """
    [A9] Gate 2–13: semua filter biner yang dapat merejeksi koin.
    Gate 1 (vol_24h + change_24h) dieksekusi di analyze_gem() sebelum fetch candle
    karena lebih murah daripada fetch.
    Return dict berisi semua nilai intermediate untuk compute_scores() dan build_signal().
    Return None jika koin tidak lolos salah satu gate.
    """
    def _reject(gate_name: str):
        if gate_stats is not None:
            gate_stats[gate_name] = gate_stats.get(gate_name, 0) + 1
        return None

    n = len(closes)

    # Gate 2: listing terlalu baru
    if n < NEW_LISTING_MIN_CANDLES:
        return _reject("G2_new_listing")
    is_new_listing = n < (NEW_LISTING_MAX_DAYS * NEW_LISTING_CANDLES_PER_DAY)   # [E1-LIST] 14×96=1344 candle

    flat_w = GEM_FLAT_WINDOW if n >= GEM_FLAT_WINDOW + GEM_BREAKOUT_WINDOW + 5 \
             else max(n - GEM_BREAKOUT_WINDOW - 3, GEM_FLAT_MIN_LEN)
    effective_min_len = GEM_FLAT_MIN_LEN_BTC_BLOCK if btc_block_buy else GEM_FLAT_MIN_LEN
    # [P5-FLAT] flat_w tidak boleh di-shrink di bawah effective_min_len dalam kondisi apapun
    if flat_w < effective_min_len:
        return _reject("G2b_flat_window_too_short")

    bs         = n - GEM_BREAKOUT_WINDOW
    flat_start = max(0, bs - flat_w)
    fc = closes[flat_start:bs];  fv = volumes[flat_start:bs]
    fh = highs[flat_start:bs];   fl = lows[flat_start:bs]

    if len(fc) < GEM_FLAT_MIN_LEN:
        return _reject("G2c_fc_too_short")

    # Gate 3: Flat Base
    ranges = [(fh[i]-fl[i])/fc[i]*100 for i in range(len(fc)) if fc[i] > 0]
    if not ranges:
        return _reject("G3_no_ranges")
    avg_flat_range = float(np.mean(ranges))
    if avg_flat_range > GEM_FLAT_MAX_BODY:
        return _reject(f"G3_flat_range_{avg_flat_range:.2f}pct")

    # Gate 3a: Flat Quality Filter [P9-FLAT]
    # Minimal FLAT_QUALITY_MIN_RATIO (80%) candle harus punya range < GEM_FLAT_MAX_BODY
    # Mencegah koin dengan 1-2 candle besar di tengah flat period lolos karena "rata-rata" masih oke
    good_candles = sum(1 for r in ranges if r < GEM_FLAT_MAX_BODY)
    flat_quality_ratio = good_candles / len(ranges) if ranges else 0.0
    if flat_quality_ratio < FLAT_QUALITY_MIN_RATIO:
        return _reject(f"G3a_flat_quality_{flat_quality_ratio:.2f}")

    # Gate 3b: Slope filter
    flat_slope = linear_slope(fc)
    if flat_slope < FLAT_DOWNTREND_SLOPE_MAX:
        return _reject(f"G3b_slope_{flat_slope:.3f}")

    # Gate 4: Volume spike — [P8-SESS] threshold disesuaikan dengan sesi trading
    vol_baseline = float(np.mean(fv))
    if vol_baseline <= 0:
        return _reject("G4_zero_baseline")
    vol_ratio = float(np.mean(volumes[-GEM_BREAKOUT_WINDOW:])) / vol_baseline
    # [D2-CHNG] np.mean dipakai untuk breakout window — changelog v2.9 [A1-SIG] keliru
    # menyebut np.percentile(75). Kode sudah benar sejak awal, dokumentasinya yang salah.
    vol_spike_threshold = get_vol_spike_threshold()   # [P8-SESS] 2.5× prime time, 3.0× normal
    if vol_ratio < vol_spike_threshold:
        return _reject(f"G4_vol_ratio_{vol_ratio:.2f}x_thresh_{vol_spike_threshold:.1f}x")

    # Gate 5: Candle vol minimum
    candle_vol_median = float(np.median(volumes))
    if candle_vol_median < GEM_CANDLE_VOL_MIN:
        return _reject(f"G5_candle_vol_{candle_vol_median:.0f}")

    # Gate 6: Anti-pump
    if is_manipulative_pump(closes, volumes, vol_ratio, change_24h):
        return _reject("G6_anti_pump")

    # Gate 7: Breakout pct + dist from base
    price_before = float(closes[-(GEM_BREAKOUT_WINDOW + 1)])
    if price_before <= 0:
        return _reject("G7_zero_price_before")
    breakout_pct   = (price - price_before) / price_before * 100
    if breakout_pct < GEM_BREAKOUT_PCT_MIN:
        return _reject(f"G7_breakout_{breakout_pct:.2f}pct")
    flat_avg       = float(np.mean(fc))
    dist_from_base = (price - flat_avg) / flat_avg * 100 if flat_avg > 0 else 0.0
    if dist_from_base > GEM_MAX_DIST_FROM_BASE:
        return _reject(f"G7_dist_{dist_from_base:.1f}pct")
    if dist_from_base > GEM_DIST_STALE_MAX and change_24h < 5.0:
        return _reject(f"G7_stale_dist{dist_from_base:.1f}_24h{change_24h:.1f}")

    # Gate 8: RSI
    rsi_val = rsi(closes)
    if not (GEM_RSI_MIN <= rsi_val <= GEM_RSI_MAX):
        return _reject(f"G8_rsi_{rsi_val:.1f}")

    # Gate 9: EMA
    ema7  = ema(closes, 7)
    ema20 = ema(closes, 20)

    # Gate 10: MTF — [E6-BURST] micro-sleep sebelum call berikutnya
    time.sleep(API_MICRO_SLEEP_SEC)
    mtf = check_mtf(client, pair, closes_15m=closes)   # [D1-MTF] renamed from closes_4h
    if not mtf["ok"]:
        return _reject(f"G10_mtf_{','.join(mtf['detail'])}")

    # Gate 11: 90-Day High — [F4-SLEEP] micro-sleep sebelum 4-batch fetch
    time.sleep(API_MICRO_SLEEP_SEC)
    high_90d = get_high_90d(client, pair, float(np.max(highs)))
    if high_90d <= 0:
        return _reject("G11_no_high90d")
    high90d_dist = (high_90d - price) / high_90d
    if high90d_dist < GEM_HIGH90D_DIST_MIN or high90d_dist > GEM_HIGH90D_DIST_MAX:
        return _reject(f"G11_high90d_dist_{high90d_dist:.2f}")
    if high90d_dist >= 0.95:
        vol_30h_median = float(np.median(volumes[-8:])) if len(volumes) >= 8 else 0.0
        if vol_30h_median < 500.0:
            return _reject("G11_zombie")

    # Gate 12: Order book depth + spread — [F4-SLEEP] micro-sleep sebelum OB+wall calls
    time.sleep(API_MICRO_SLEEP_SEC)
    ob = check_orderbook_depth(client, pair)
    if not ob["ok"]:
        return _reject(f"G12_spread_{ob['spread_pct']:.2f}pct")

    # Gate 12b: Volume Wall Consistency
    wall = check_volume_wall(client, pair, candle_vol_median)
    if wall["fake_pump_warn"]:
        log(f"  ⚠ {pair} fake pump warning — volume spike tapi bid wall nyaris kosong", "warn")
        return _reject("G12b_fake_pump_wall")

    # Gate 13: ATR minimum [FIX-E6]
    atr_val = atr(closes, highs, lows)
    atr_pct = atr_val / price * 100 if price > 0 else 0.0
    if atr_pct < GEM_ATR_PCT_MIN:
        return _reject("G13_atr_too_low")   # [C3-GATE] was bare return None — invisible di diagnostics

    return {
        "fc": fc, "fv": fv, "flat_avg": flat_avg,
        "is_new_listing": is_new_listing,
        "avg_flat_range": avg_flat_range,
        "flat_slope": flat_slope,
        "flat_quality_ratio": flat_quality_ratio,   # [P9-FLAT]
        "vol_baseline": vol_baseline,
        "vol_ratio": vol_ratio,
        "candle_vol_median": candle_vol_median,
        "breakout_pct": breakout_pct,
        "dist_from_base": dist_from_base,
        "rsi_val": rsi_val,
        "ema7": ema7, "ema20": ema20,
        "mtf": mtf,
        "high_90d": high_90d,
        "high90d_dist": high90d_dist,
        "ob": ob,
        "wall": wall,       # [S3-WALL]
        "atr_val": atr_val,
        "atr_pct": atr_pct,
    }


def compute_scores(
    client: SpotApi,
    pair: str, closes: np.ndarray, highs: np.ndarray, lows: np.ndarray,
    volumes: np.ndarray, price: float,
    vol_24h: float,
    gate: dict,
    ticker_map: dict[str, float],
) -> dict:
    """
    [A9] Hitung semua sub-score dari data yang sudah lolos gate filter.
    Tidak ada early-return — semua score dihitung dan dikembalikan sebagai dict.

    [S1-FREQ] trade_freq: deteksi lonjakan transaksi/menit dari exchange lain
    [S2-MCAP] micro_mcap: identifikasi koin ultra micro untuk bonus ULTRA GEM
    [S3-WALL] wall score sudah ada di gate dict dari run_gate_filters()
    """
    dormancy_score = 3 if gate["avg_flat_range"] < 0.8 else 2 if gate["avg_flat_range"] < 1.5 else 1
    vol_score      = 3 if gate["vol_ratio"] >= 30 else 2 if gate["vol_ratio"] >= 15 else 1
    breakout_score = 3 if gate["breakout_pct"] >= 20 else 2 if gate["breakout_pct"] >= 10 else 1
    rsi_score      = 2 if gate["rsi_val"] < 45 else 1 if gate["rsi_val"] < 55 else 0
    high90d_score  = (3 if gate["high90d_dist"] >= 0.85
                      else 2 if gate["high90d_dist"] >= 0.70
                      else 1 if gate["high90d_dist"] >= 0.50 else 0)
    ema_score      = (1 if price > gate["ema7"] else 0) + (1 if gate["ema7"] > gate["ema20"] else 0)

    has_sweep           = detect_liquidity_sweep(closes, highs, lows)
    macd_line, macd_sig = macd(closes)
    macd_bull           = macd_line > macd_sig

    sector_info         = get_sector_momentum(pair, ticker_map)
    vol30_score         = calc_vol30_score(gate["vol_ratio"], prefetched_volumes=volumes)   # [G1-PARAM]
    narrative_bonus, narrative_match = get_narrative_bonus(pair, sector_info["sector"])

    # [S1-FREQ] Hitung frekuensi transaksi — deteksi bocoran bot exchange lain
    trade_freq = check_trade_frequency(client, pair)

    # [S2-MCAP] Estimasi micro MCap — bonus untuk ultra gem
    micro_mcap = estimate_micro_mcap(price, vol_24h, sector_info["sector"])

    # [S3-WALL] Wall score sudah dihitung di run_gate_filters()
    wall_score = gate["wall"]["wall_score"]

    # [E5-TIER] Scoring split: BASE (max 32) untuk tier assignment, BONUS (max 5) display only.
    # Mencegah koin dengan sinyal teknikal lemah masuk MOONSHOT hanya karena S1/S2/S3 bonus.
    base_score = (
        dormancy_score + vol_score + breakout_score + rsi_score
        + high90d_score + ema_score
        + gate["mtf"]["mtf_bonus"] + gate["mtf"]["tf_alignment"]
        + (2 if has_sweep else 0)
        + (1 if macd_bull else 0)
        + gate["ob"]["depth_score"]
        + sector_info["sector_bonus"]
        + vol30_score + narrative_bonus
    )   # max = 32

    bonus_score = (
        trade_freq["freq_score"]    # [S1] max +2
        + micro_mcap["mcap_score"]  # [S2] max +1
        + wall_score                # [S3] max +2
    )   # max = 5

    total_score = base_score + bonus_score   # max = 37

    return {
        "dormancy_score": dormancy_score, "vol_score": vol_score,
        "breakout_score": breakout_score, "rsi_score": rsi_score,
        "high90d_score": high90d_score, "ema_score": ema_score,
        "has_sweep": has_sweep, "macd_bull": macd_bull,
        "sector_info": sector_info,
        "vol30_score": vol30_score,
        "narrative_bonus": narrative_bonus,
        "narrative_match": narrative_match,
        "trade_freq": trade_freq,     # [S1]
        "micro_mcap": micro_mcap,     # [S2]
        "wall_score": wall_score,     # [S3]
        "base_score": base_score,     # [E5-TIER] max 32 — dipakai untuk tier assignment
        "bonus_score": bonus_score,   # [E5-TIER] max 5  — S1+S2+S3, display only
        "total_score": total_score,   # max 37
    }


def build_signal(
    pair: str, price: float, vol_24h: float, change_24h: float,
    gate: dict, scores: dict,
) -> dict | None:
    """
    [A9] Assembling dict sinyal final dari gate results dan scores.
    Hitung tier, SL/TP, R/R. Return None jika R/R tidak memenuhi minimum.

    [F5-EXIT] Trailing Take Profit:
    - Ambil modal di TP1 (kenaikan 10–20%)
    - Setelah TP1 tercapai, geser SL ke titik entry (breakeven)
    - Biarkan sisa posisi berjalan bebas menuju TP2/TP3
    - Efek: "tiket gratis" — modal sudah aman, menunggu moonshot tanpa risiko
    """
    total_score = scores["total_score"]
    base_score  = scores["base_score"]   # [E5-TIER] tier dari base score, bukan total

    # [E5-TIER] Tier ditentukan dari BASE score (max 32) — bonus S1/S2/S3 tidak menaikkan tier.
    # Koin harus kuat secara teknikal untuk masuk MOONSHOT, bukan karena trade freq spike saja.
    if   base_score >= GEM_TIER["MOONSHOT"]: tier = "MOONSHOT"
    elif base_score >= GEM_TIER["GEM"]:      tier = "GEM"
    elif base_score >= GEM_TIER["WATCH"]:    tier = "WATCH"
    else: return None

    atr_val      = gate["atr_val"]
    sl_atr_based = atr_val > 0
    sl_pct       = max(GEM_SL_MIN_PCT, min(GEM_SL_MAX_PCT,
                       (atr_val * GEM_SL_ATR_MULTIPLIER) / price)) \
                   if atr_val > 0 else 0.08

    # [A2] Teruskan high_90d ke calc_dynamic_tp agar TP3 bisa dikap dengan benar
    tp1, tp2, tp3 = calc_dynamic_tp(price, gate["high90d_dist"], gate["high_90d"])

    entry   = price
    sl      = round(entry * (1 - sl_pct), 8)
    sl_dist = entry - sl
    # [E7-SL] Guard defensif — sl_pct = max(0.05,...) menjamin sl < entry
    # sehingga sl_dist selalu > 0. Guard ini tidak seharusnya trigger
    # dalam operasi normal, tapi dipertahankan sebagai safety net.
    if sl_dist <= 0:
        return None

    # [F5-EXIT] SL geser ke breakeven setelah TP1 tercapai
    # sl_breakeven = entry (titik modal kembali, posisi menjadi risk-free)
    sl_breakeven = entry

    # [FIX-E2] R/R dihitung dari TP2 — TP2 mewakili target realistis setup ini
    rr_ratio = round((tp2 - entry) / sl_dist, 1)
    if rr_ratio < GEM_MIN_RR:
        return None

    mcap_tier     = estimate_mcap_tier(vol_24h, scores["sector_info"]["sector"])
    ultra_low_liq = vol_24h < GEM_VOL_MAX_ULTRA

    # [P4-MCAP] Auto-upgrade tier ke MOONSHOT dari estimasi MCap DIHAPUS.
    # Estimasi MCap dari vol/turnover_assumed terlalu tidak akurat untuk dijadikan
    # dasar upgrade tier — bisa generate false MOONSHOT dari koin meme yang
    # vol-nya tinggi sehari tapi MCap estimasinya rendah karena turnover 100%.
    # mcap_label tetap ada sebagai display info dan score bonus saja.
    micro_mcap    = scores["micro_mcap"]

    return {
        "pair": pair, "tier": tier, "score": total_score,
        "entry": entry, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "sl": sl, "sl_pct": round(sl_pct * 100, 2), "sl_atr_based": sl_atr_based,
        "sl_breakeven": sl_breakeven,
        "rr": rr_ratio, "rsi": round(gate["rsi_val"], 1),
        "vol_ratio": round(gate["vol_ratio"], 1),
        "breakout_pct": round(gate["breakout_pct"], 2),
        "avg_flat_range": round(gate["avg_flat_range"], 2),
        "flat_slope": round(gate["flat_slope"], 3),
        "high90d_dist_pct": round(gate["high90d_dist"] * 100, 1),
        # [E3-ATH] ath_dist_pct legacy field dihapus — high90d_dist_pct adalah satu-satunya field
        "dist_from_base": round(gate["dist_from_base"], 1),
        "atr_pct": round(gate["atr_pct"], 2),
        "is_new_listing": gate["is_new_listing"],
        "has_sweep": scores["has_sweep"],
        "macd_bull": scores["macd_bull"],
        "change_24h": round(change_24h, 2),
        "rsi_fast": round(gate["mtf"]["rsi_fast"], 1),   # [P2-MTF] RSI 5m konfirmasi cepat
        "rsi_mid": round(gate["mtf"]["rsi_mid"], 1),     # [P2-MTF] RSI 1h konfirmasi medium
        "trend_1h": gate["mtf"]["trend_1h"],
        "spread_pct": gate["ob"]["spread_pct"],
        "depth_ratio": gate["ob"]["depth_ratio"],
        "depth_score": gate["ob"]["depth_score"],
        "tf_alignment": gate["mtf"]["tf_alignment"],
        "sector": scores["sector_info"]["sector"],
        "sector_hot": scores["sector_info"]["sector_hot"],
        "sector_bonus": scores["sector_info"]["sector_bonus"],
        "vol30_score": scores["vol30_score"],
        "narrative": scores["narrative_match"],
        "narrative_bonus": scores["narrative_bonus"],
        "ultra_low_liq": ultra_low_liq,
        "mcap_tier": mcap_tier,
        "dormancy_score": scores["dormancy_score"],
        "vol_score": scores["vol_score"],
        "breakout_score": scores["breakout_score"],
        "candle_vol_median": round(gate["candle_vol_median"], 2),
        "flat_vol_median": round(gate["vol_baseline"], 4),
        "flat_quality_ratio": round(gate["flat_quality_ratio"] * 100, 1),  # [P9-FLAT] dalam %
        "base_score":  scores["base_score"],    # [E5-TIER] max 32 — dasar tier
        "bonus_score": scores["bonus_score"],   # [E5-TIER] max 5  — S1+S2+S3
        # ── Fitur v3.1–v3.7 ──────────────────────────────────────────
        "tx_per_min":      scores["trade_freq"]["tx_per_min"],       # [S1]
        "freq_spike":      scores["trade_freq"]["freq_spike"],       # [S1]
        "freq_strong":     scores["trade_freq"]["freq_strong"],      # [S1]
        "freq_score":      scores["trade_freq"]["freq_score"],       # [S1]
        "est_mcap_usd":    micro_mcap["est_mcap_usd"],               # [S2]
        "mcap_label":      micro_mcap["mcap_label"],                 # [S2]
        "is_ultra_gem":    micro_mcap["is_ultra_gem"],               # [S2]
        "is_micro":        micro_mcap["is_micro"],                   # [S2]
        "wall_usdt":       gate["wall"]["wall_usdt"],                # [S3]
        "wall_ratio":      gate["wall"]["wall_ratio"],               # [S3]
        "wall_legit":      gate["wall"]["wall_legit"],               # [S3]
        "wall_score":      scores["wall_score"],                     # [S3]
    }


def analyze_gem(
    client: SpotApi, pair: str, price: float,
    vol_24h: float, change_24h: float,
    ticker_map: dict[str, float] | None = None,
    btc_block_buy: bool = False,
    gate_stats: dict | None = None,   # [DIAG]
) -> dict | None:
    """
    [A9] Orchestrator tipis — delegasikan ke run_gate_filters(), compute_scores(),
    build_signal(). Cyclomatic complexity turun dari ~15 ke ~3.
    """
    # Gate 1: Vol 24h + change 24h — paling murah, tidak perlu fetch candle
    if not (GEM_VOL_MIN <= vol_24h <= GEM_VOL_MAX) or change_24h > GEM_MAX_CHANGE_24H:
        if gate_stats is not None:
            gate_stats["G1_vol_or_change"] = gate_stats.get("G1_vol_or_change", 0) + 1
        return None

    data = fetch_candles(client, pair)
    if data is None:
        if gate_stats is not None:
            gate_stats["G0_fetch_fail"] = gate_stats.get("G0_fetch_fail", 0) + 1
        return None
    closes, highs, lows, volumes = data

    gate = run_gate_filters(
        client, pair, price, vol_24h, change_24h,
        closes, highs, lows, volumes,
        btc_block_buy=btc_block_buy,
        gate_stats=gate_stats,
    )
    if gate is None:
        return None

    scores = compute_scores(
        client, pair, closes, highs, lows, volumes, price,
        vol_24h,
        gate, ticker_map or {},
    )

    sig = build_signal(pair, price, vol_24h, change_24h, gate, scores)
    if sig is None and gate_stats is not None:
        gate_stats["G_final_rr_or_tier"] = gate_stats.get("G_final_rr_or_tier", 0) + 1
    return sig


# ══════════════════════════════════════════════════════════════════
#  TELEGRAM FORMATTER
# ══════════════════════════════════════════════════════════════════

def format_signal(sig: dict) -> str:
    pair  = sig["pair"].replace("_USDT", "/USDT")
    entry = sig["entry"]
    tp1, tp2, tp3, sl = sig["tp1"], sig["tp2"], sig["tp3"], sig["sl"]
    tier  = sig["tier"]

    pct_tp1 = (tp1 - entry) / entry * 100
    pct_tp2 = (tp2 - entry) / entry * 100
    pct_tp3 = (tp3 - entry) / entry * 100
    pct_sl  = (entry - sl)  / entry * 100

    now       = datetime.now(WIB)
    valid     = (now + timedelta(hours=GEM_DEDUP_HOURS)).strftime("%d/%m %H:%M WIB")
    tier_icon = {"MOONSHOT": "🚀", "GEM": "💎", "WATCH": "👁"}.get(tier, "🎯")
    sl_label  = f"(-{pct_sl:.1f}% · {'ATR-based' if sig.get('sl_atr_based') else 'fixed'})"

    ctx = []
    if sig["ultra_low_liq"]:
        ctx.append("⚠️ <b>ULTRA LOW LIQUIDITY</b> — vol 24h &lt;$150K, slippage &amp; exit risk tinggi")
    if sig.get("is_ultra_gem"):
        ctx.append(f"💎💎 <b>ULTRA GEM DETECTED</b> — est. MCap {sig.get('mcap_label','?')} | koin ini masih bisa 100x")
    elif sig.get("is_micro"):
        ctx.append(f"💎 Micro Cap terdeteksi — {sig.get('mcap_label','?')} | ruang besar untuk naik")
    if sig.get("freq_strong"):
        ctx.append(f"🚨 <b>BOT EXCHANGE LAIN AKTIF</b> — {sig.get('tx_per_min',0):.0f} tx/menit (normal: 1–3) | beli sekarang sebelum terlambat")
    elif sig.get("freq_spike"):
        ctx.append(f"⚡ Trade frequency spike — {sig.get('tx_per_min',0):.0f} tx/menit | aktivitas awal terdeteksi")
    if sig.get("wall_legit"):
        ctx.append(f"🧱 <b>Buy Wall terkonfirmasi</b> — ${sig.get('wall_usdt',0):.0f} USDT bid wall ({sig.get('wall_ratio',0):.1f}× median vol) | breakout LEGIT")
    if sig["is_new_listing"]:
        ctx.append("🆕 Pair relatif baru (<14 hari) — volatilitas lebih tinggi")
    if sig["has_sweep"]:
        ctx.append("🧲 Liquidity sweep terdeteksi — smart money sudah masuk")
    if sig["macd_bull"]:
        ctx.append("📊 MACD micro bullish — momentum awal terbentuk")
    if sig["sector_hot"]:
        ctx.append(f"🔥 Sektor <b>{sig['sector']}</b> sedang hot — rotation aktif")
    if sig["depth_score"] >= 2:
        ctx.append(f"📚 Bid wall kuat (depth ratio {sig['depth_ratio']:.1f}×) — akumulasi terdeteksi")
    elif sig["depth_score"] == 1:
        ctx.append(f"📚 Bid sedikit dominan (depth ratio {sig['depth_ratio']:.1f}×)")
    if sig["tf_alignment"] == 3:
        ctx.append("🎯 Full TF alignment (15m+5m+1h bullish) — konfluensi kuat")
    elif sig["tf_alignment"] == 2:
        ctx.append("🎯 2/3 TF aligned bullish")
    if sig["avg_flat_range"] < 1.0:
        ctx.append(f"😴 Deep sleep ({sig['avg_flat_range']:.1f}% avg range) — energi tersimpan panjang")
    elif sig["avg_flat_range"] < 2.0:
        ctx.append(f"😴 Flat base jelas ({sig['avg_flat_range']:.1f}% avg range)")
    if sig["vol30_score"] >= 2:
        ctx.append("📈 Volume anomaly 30d — breakout ini jauh di atas normal historis")
    elif sig["vol30_score"] == 1:
        ctx.append("📈 Volume di atas rata-rata 30 hari")
    if sig["narrative_bonus"]:
        ctx.append(f"🗣️ Narasi aktif terdeteksi: <b>{sig['narrative']}</b>")

    score_d = (
        f"Dormancy {sig['dormancy_score']}/3 · Vol {sig['vol_score']}/3 "
        f"· Breakout {sig['breakout_score']}/3 · TF Align {sig['tf_alignment']}/3 "
        f"· Vol30 {sig['vol30_score']}/2 · Narrative {sig['narrative_bonus']}/1 "
        f"· Freq {sig.get('freq_score',0)}/2 · MCap {1 if sig.get('is_ultra_gem') else 0}/1 · Wall {sig.get('wall_score',0)}/2"
    )

    # Session info untuk display
    session_label = "🔥 Prime Time" if is_prime_session() else "🌙 Off-Peak"

    # [A4] Tier-aware position sizing — bukan satu ukuran untuk semua
    # MOONSHOT: lebih kecil karena probabilitas TP3 rendah & liquidity tipis
    # ultra_low_liq mendapat warning tambahan terlepas dari tier
    if tier == "GEM":
        alloc = "0.5–1%"
    elif tier == "MOONSHOT":
        alloc = "0.25–0.5%"
    else:  # WATCH
        alloc = "0.25–0.5%"
    liq_note = " ⚠️ kurangi 50% — ultra low liq" if sig["ultra_low_liq"] else ""
    pos_size_line = f"<i>· Alokasi maks {alloc} modal{liq_note}</i>"

    return (
        f"{tier_icon} <b>GEM HUNTER — {tier}</b>  🟢 BUY\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Pair    : <b>{pair}</b>  [15m]\n"
        f"⏰ Valid : {now.strftime('%d/%m %H:%M')} → {valid}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Entry   : <b>${entry:.8f}</b>  <i>≈ {fmt_idr(entry)}</i>\n"
        f"TP1     : <b>${tp1:.8f}</b>  <i>≈ {fmt_idr(tp1)}</i>  <i>(+{pct_tp1:.0f}% → ambil modal)</i>\n"
        f"TP2     : <b>${tp2:.8f}</b>  <i>≈ {fmt_idr(tp2)}</i>  <i>(+{pct_tp2:.0f}%)</i>\n"
        f"TP3     : <b>${tp3:.8f}</b>  <i>(+{pct_tp3:.0f}% moonshot)</i>\n"
        f"SL      : <b>${sl:.8f}</b>  <i>≈ {fmt_idr(sl)}</i>  <i>{sl_label}</i>\n"
        f"SL-BE   : <b>${sig['sl_breakeven']:.8f}</b>  <i>(geser ke sini setelah TP1 — tiket gratis)</i>\n"
        f"R/R     : <b>1:{sig['rr']}</b> (vs TP2)  |  ATR: {sig['atr_pct']:.2f}%\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Vol spike    : <b>{sig['vol_ratio']:.1f}×</b> mean flat  |  <b>{sig['vol30_score']}/2</b> vs 30d\n"
        f"📈 Breakout     : <b>+{sig['breakout_pct']:.1f}%</b> dari base  |  Candle vol: ${sig.get('candle_vol_median', 0):.0f}/15m\n"
        f"💤 Dormancy     : {sig['avg_flat_range']:.1f}% avg range  |  Flat quality: {sig.get('flat_quality_ratio', 0):.0f}% candles ok\n"
        f"📉 90d High dist : <b>{sig['high90d_dist_pct']:.0f}%</b> di bawah puncak ~30d  |  {session_label}\n"
        f"📚 OB Depth     : {sig['depth_ratio']:.1f}× bid/ask  |  Sektor: {sig['sector']}\n"
        f"🧱 Buy Wall     : ${sig.get('wall_usdt',0):.0f} USDT  ({'✅ Legit' if sig.get('wall_legit') else '⚠️ Tipis'})\n"
        f"⚡ Trade Freq   : <b>{sig.get('tx_per_min',0):.0f} tx/mnt</b>  ({'🚨 Bot aktif!' if sig.get('freq_strong') else '⚡ Spike' if sig.get('freq_spike') else '😴 Normal'})\n"
        f"💰 Est. MCap    : <b>{sig.get('mcap_label', '—')}</b>\n"
        f"RSI 15m : <b>{sig['rsi']}</b>  |  5m: {sig['rsi_fast']}  |  1h: {sig['rsi_mid']}\n"
        f"TF Align: {'✅✅✅' if sig['tf_alignment']==3 else '✅✅⬜' if sig['tf_alignment']==2 else '✅⬜⬜'}  "
        f"Trend 1h: {'✅' if sig['trend_1h'] else '⚠️'}\n"
        f"MCap    : <i>{sig['mcap_tier']}</i>  |  24h: {sig['change_24h']:+.1f}%\n"
        f"Spread  : <b>{sig['spread_pct']:.2f}%</b>  |  Score: <b>{sig['score']}/37</b>  <i>(base {sig.get('base_score',0)}/32 + bonus {sig.get('bonus_score',0)}/5)</i>\n"
        f"<i>({score_d})</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{chr(10).join(ctx) if ctx else '—'}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <b>Extreme High Risk Setup</b>\n"
        f"{pos_size_line}\n"
        f"<i>· Hold 1–7 hari — bukan scalp</i>\n"
        f"<i>· 🎯 TRAILING TP: Ambil modal di TP1, geser SL ke SL-BE</i>\n"
        f"<i>· Sisa posisi jalan bebas ke TP2/TP3 tanpa risiko modal</i>\n"
        f"<i>· SL wajib dipasang — koin kecil bisa dump 50% dalam jam</i>\n"
        f"<i>· TP3 bersifat spekulatif — berbasis puncak 90d, bukan ATH sejati</i>\n"
        f"<i>· Bukan rekomendasi finansial</i>"
    )


def format_summary(scanned: int, skipped: int, found: int, sent: int,
                   halted: bool = False) -> str:
    now = datetime.now(WIB).strftime("%d/%m/%Y %H:%M WIB")
    if halted:
        return f"🛑 <b>GEM HUNTER v3.7 — HALT</b>\nBTC crash. Scan dilewati.\n<i>{now}</i>"
    return (
        f"💎 <b>GEM HUNTER v3.7 Audit Complete Scan Selesai</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f"Pairs scanned   : {scanned}\nDiluar vol range: {skipped}\n"
        f"Kandidat gem    : {found}\nSignal terkirim : <b>{sent}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n<i>Setup ini butuh 1–7 hari.</i>\n<i>{now}</i>"
    )


# ══════════════════════════════════════════════════════════════════
#  AUTO OUTCOME TRACKER [FIX-7 + FIX-B2]
# ══════════════════════════════════════════════════════════════════

def check_outcomes(client: SpotApi) -> int:
    """
    [FIX-B2] v2.1 hanya cek harga SEKARANG — sinyal yang TP-nya sudah
    tercapai kemarin lalu harga turun tidak pernah dicatat sebagai WIN.
    v2.2 menyimpan high_watermark per sinyal: TP dievaluasi dari harga
    tertinggi sejak sinyal, SL dari harga sekarang.
    """
    updated = 0
    try:
        res = (
            sb().table("gem_signals")
            .select("id,pair,entry,tp1,tp2,tp3,sl,vol_ratio,created_at,high_watermark,vol_weak_alerted")  # flat_vol_median di-drop dari SELECT — kolom opsional, di-get() per row dengan .get()
            .is_("outcome", "null")
            .order("created_at", desc=False)
            .limit(50)
            .execute()
        )
        pending = res.data or []
        if not pending:
            return 0
        log(f"Outcome tracker: {len(pending)} pending...")

        # [A6] Batch fetch: satu panggilan list_tickers untuk semua pair aktif.
        # Sebelumnya: N × list_tickers(currency_pair=pair) + 0.3s sleep = ≥15s untuk 50 sinyal.
        # Sekarang: 1 panggilan saja → membangun price_map dan high_map dari data segar.
        # [A10] Selain last price, ambil juga high candle 4h terkini untuk update watermark
        # — tangkap spike intra-run yang tidak terdeteksi jika hanya pakai "last" price.
        price_map: dict[str, float] = {}
        high_map: dict[str, float] = {}
        try:
            all_tickers = gate_retry(client.list_tickers) or []
            for tk in all_tickers:
                p = getattr(tk, "currency_pair", None)
                if p:
                    try:
                        price_map[p] = float(tk.last or 0)
                        # high field tersedia di ticker Gate.io; fallback ke last jika None
                        high_map[p]  = float(tk.high_24h or tk.last or 0)
                    except Exception:
                        pass
            log(f"  Batch ticker fetch: {len(price_map)} pairs dimuat untuk outcome check")
        except Exception as e:
            log(f"  Batch ticker fetch gagal: {e} — outcome check mungkin terlewat", "warn")
            # Lanjut dengan price_map kosong; per-pair akan dapat cur=0 dan di-skip

        for sig in pending:
            pair, sig_id = sig["pair"], sig["id"]
            entry, tp1, tp2, tp3, sl = (float(sig[k]) for k in ("entry","tp1","tp2","tp3","sl"))
            # [FIX-B2] Ambil high_watermark tersimpan (default ke entry jika NULL)
            high_wm  = float(sig["high_watermark"] or entry)
            created  = datetime.fromisoformat(sig["created_at"].replace("Z", "+00:00"))
            # [ISU-4] Pakai total_seconds()/86400 bukan .days — .days memotong jam,
            # sehingga sinyal dibuat 23:59 Senin baru punya age_days=1 jam 00:00 Rabu
            # padahal sudah 24+ jam. total_seconds() menghitung durasi penuh.
            age_days = (datetime.now(timezone.utc) - created).total_seconds() / 86400

            if age_days >= 7:
                sb().table("gem_signals").update({
                    "outcome": "EXPIRED",
                    "outcome_at": datetime.now(timezone.utc).isoformat(),
                }).eq("id", sig_id).execute()
                updated += 1
                log(f"  ⏰ {pair} EXPIRED", "warn")
                continue

            # [A6] Harga diambil dari price_map yang sudah di-batch sebelum loop —
            # tidak ada individual list_tickers() call di sini.
            cur = price_map.get(pair, 0.0)
            if cur <= 0:
                continue

            # [FIX-B2] Update high_watermark — gunakan max(cur, high_24h) agar
            # spike intra-run yang sudah turun kembali tetap terekam [A10].
            # high_map berisi high 24h dari ticker Gate.io; jika pair tidak ada
            # di map (batch fetch gagal) fallback ke cur saja.
            effective_high = max(cur, high_map.get(pair, cur))
            if effective_high > high_wm:
                high_wm = effective_high
                try:
                    sb().table("gem_signals").update(
                        {"high_watermark": effective_high}
                    ).eq("id", sig_id).execute()
                except Exception as e:
                    log(f"  watermark update [{pair}]: {e}", "warn")

            # [UPG-C4] Post-signal volume weakening alert
            # Jika sinyal masih pending < 2 hari, cek apakah volume sudah melemah
            # [FIX-D8] Cek flag vol_weak_alerted di DB — agar alert tidak repeat tiap run
            # [ISU-2] Logika diperbaiki: baseline yang benar adalah vol saat sinyal
            #   (= vol_ratio × vol_baseline flat), bukan median seluruh window terkini.
            #   Median 34 candle terakhir mencakup candle breakout itu sendiri, sehingga
            #   hampir semua candle post-signal normal akan terdeteksi sebagai "lemah".
            #   Komparasi yang benar: current_vol vs signal_vol (puncak breakout).
            if age_days < 2:
                try:
                    already_alerted = bool(sig.get("vol_weak_alerted"))
                    if not already_alerted:
                        raw_recent = gate_retry(client.list_candlesticks,
                                                currency_pair=pair, interval=GEM_TF, limit=40)
                        if raw_recent and len(raw_recent) >= 20:
                            vols_all    = np.array([float(c[1]) for c in raw_recent], dtype=float)
                            current_vol = float(vols_all[-1])
                            # [MEDIUM-6] Gunakan flat_vol_median yang disimpan saat sinyal dibuat (v2.8+).
                            # Ini adalah baseline vol flat period yang benar, dihitung dari fv median
                            # di analyze_gem() — jauh lebih akurat dari estimasi post-hoc dari candle terkini.
                            # Fallback ke estimasi lama hanya jika data DB tidak tersedia (sinyal pre-v2.8).
                            sig_vol_ratio    = float(sig.get("vol_ratio") or 0)
                            db_flat_vol      = sig.get("flat_vol_median")
                            if db_flat_vol and float(db_flat_vol) > 0 and sig_vol_ratio > 0:
                                # Cara terbaik: pakai baseline tersimpan langsung dari DB
                                flat_median_vol = float(db_flat_vol)
                                signal_vol      = flat_median_vol * sig_vol_ratio
                            elif sig_vol_ratio > 0:
                                # Fallback v2.7: estimasi flat_baseline dari candle terkini
                                flat_median_vol = float(np.median(vols_all[:-8])) if len(vols_all) >= 16 else 0.0
                                signal_vol      = flat_median_vol * sig_vol_ratio if flat_median_vol > 0 else 0.0
                            else:
                                # Fallback akhir: max 6 candle paling awal di window
                                signal_vol = float(np.max(vols_all[:6]))
                            threshold_vol = signal_vol / VOL_WEAKENING_RATIO
                            if signal_vol > 0 and current_vol < threshold_vol:
                                tg(
                                    f"⚠️ <b>VOLUME WEAKENING</b>\n"
                                    f"Pair   : <b>{pair.replace('_USDT','/USDT')}</b>\n"
                                    f"Vol sekarang {current_vol:.0f} USDT vs signal peak ~{signal_vol:.0f} USDT "
                                    f"({current_vol/signal_vol:.2f}× — di bawah 1/{VOL_WEAKENING_RATIO:.0f} signal vol)\n"
                                    f"<i>Momentum breakout sudah melemah — pertimbangkan reduce position</i>\n"
                                    f"<i>{datetime.now(WIB).strftime('%d/%m/%Y %H:%M WIB')}</i>"
                                )
                                log(f"  ⚠ {pair} volume weakening alert", "warn")
                                # [FIX-D8] Set flag agar alert tidak dikirim lagi di run berikutnya
                                try:
                                    sb().table("gem_signals").update(
                                        {"vol_weak_alerted": True}
                                    ).eq("id", sig_id).execute()
                                except Exception as db_err:
                                    log(f"  vol_weak flag update [{pair}]: {db_err}", "warn")
                except Exception as e:
                    log(f"  vol_weakening check [{pair}]: {e}", "warn")

            # [FIX-B2] TP dievaluasi dari watermark, SL dari harga sekarang
            outcome = outcome_pct = None
            if   high_wm >= tp3:
                outcome, outcome_pct = "WIN_TP3", round((high_wm - entry) / entry * 100, 2)
            elif high_wm >= tp2:
                outcome, outcome_pct = "WIN_TP2", round((high_wm - entry) / entry * 100, 2)
            elif high_wm >= tp1:
                outcome, outcome_pct = "WIN_TP1", round((high_wm - entry) / entry * 100, 2)
            elif cur <= sl:
                outcome, outcome_pct = "LOSS_SL", round((cur - entry) / entry * 100, 2)

            # [F5-EXIT] Trailing TP: jika high_watermark sudah sentuh TP1 tapi outcome
            # belum final (belum SL atau TP2+), update SL di DB ke breakeven (= entry).
            # Ini dilakukan terpisah dari outcome — posisi masih open tapi sudah risk-free.
            if outcome is None and high_wm >= tp1:
                try:
                    sl_be = float(sig.get("sl_breakeven") or entry)
                    if cur > sl_be:   # harga masih di atas breakeven — update SL jika belum
                        sb().table("gem_signals").update(
                            {"sl": sl_be, "sl_breakeven_active": True}
                        ).eq("id", sig_id).execute()
                        log(f"  🎯 {pair} TP1 sudah tersentuh — SL digeser ke breakeven ${sl_be:.8f}", "ok")
                except Exception as sl_err:
                    log(f"  trailing SL update [{pair}]: {sl_err}", "warn")

            if outcome:
                # [A7] Bungkus persistence block dalam try-except. Sebelumnya jika
                # Supabase drop saat update, outcome sudah dihitung di memori dan
                # log/TG sudah terkirim tapi DB tidak terupdate — sinyal tetap
                # "pending" dan akan dievaluasi ulang di run berikutnya, berpotensi
                # kirim notifikasi duplikat ke pengguna.
                try:
                    sb().table("gem_signals").update({
                        "outcome": outcome, "outcome_pct": outcome_pct,
                        "outcome_at": datetime.now(timezone.utc).isoformat(),
                    }).eq("id", sig_id).execute()
                    updated += 1
                    icon = "✅" if "WIN" in outcome else "❌"
                    tg(
                        f"{icon} <b>OUTCOME UPDATE</b>\n"
                        f"Pair   : <b>{pair.replace('_USDT','/USDT')}</b>\n"
                        f"Result : <b>{outcome}</b>  ({outcome_pct:+.1f}%)\n"
                        f"Entry  : ${entry:.8f}  →  Peak: ${high_wm:.8f}  |  Now: ${cur:.8f}\n"
                        f"<i>{datetime.now(WIB).strftime('%d/%m/%Y %H:%M WIB')}</i>"
                    )
                    log(f"  {icon} {pair} → {outcome} ({outcome_pct:+.1f}%)", "ok")
                    time.sleep(TG_OUTCOME_SLEEP_SEC)   # [FIX-10]
                except Exception as db_err:
                    log(f"  outcome persist [{pair}] DB error: {db_err} — akan dicoba ulang run berikutnya", "error")

            # [A6] sleep per-pair dihapus — tidak ada individual API call lagi

    except Exception as e:
        log(f"check_outcomes: {e}", "warn")
    return updated


# ══════════════════════════════════════════════════════════════════
#  WIN RATE REPORT [FIX-8]
# ══════════════════════════════════════════════════════════════════

def send_winrate_report() -> None:
    """
    Win rate report dengan breakdown TP1/TP2/TP3 per tier.

    [KRITIS-2] Tambah dedup check: simpan key 'winrate_report_YYYYMMDD' ke gem_dedup
    agar laporan tidak terkirim lebih dari 1× per hari meskipun GitHub Actions
    berjalan setiap 30 menit dan semua run di jam 08:xx memenuhi kondisi jam == 8.
    """
    today_key = f"winrate_report_{datetime.now(WIB).strftime('%Y%m%d')}"
    try:
        # Cek apakah laporan hari ini sudah pernah terkirim
        dedup_check = sb().table("gem_dedup").select("pair").eq("pair", today_key).execute()
        if dedup_check.data:
            log("Win rate report hari ini sudah terkirim — skip", "info")
            return
    except Exception as e:
        log(f"winrate_report dedup check: {e}", "warn")
        # Jika cek gagal, tetap lanjut daripada tidak kirim sama sekali

    try:
        res = (
            sb().table("gem_signals")
            .select("tier,outcome,outcome_pct")
            .not_.is_("outcome", "null")
            .execute()
        )
        signals = res.data or []
        if len(signals) < WINRATE_MIN_SIGNALS:
            return

        total   = len(signals)
        wins    = [s for s in signals if s["outcome"] and "WIN" in s["outcome"]]
        losses  = [s for s in signals if s["outcome"] == "LOSS_SL"]
        expired = [s for s in signals if s["outcome"] == "EXPIRED"]
        tp_hits = {t: len([s for s in signals if s["outcome"] == f"WIN_{t}"])
                   for t in ("TP1", "TP2", "TP3")}

        win_rate = len(wins) / total * 100 if total else 0
        avg_win  = float(np.mean([s["outcome_pct"] for s in wins  if s["outcome_pct"]])) \
                   if wins  else 0.0
        avg_loss = float(np.mean([s["outcome_pct"] for s in losses if s["outcome_pct"]])) \
                   if losses else 0.0

        tier_lines = ""
        weak_tiers = []
        for t in ("MOONSHOT", "GEM", "WATCH"):
            ts = [s for s in signals if s["tier"] == t]
            tw = [s for s in ts if s["outcome"] and "WIN" in s["outcome"]]
            if ts:
                wr = len(tw) / len(ts) * 100
                tier_lines += f"{t:<10}: {len(tw)}/{len(ts)} ({wr:.0f}%)\n"
                # [UPG-C5] Tandai tier yang win rate-nya di bawah 30%
                if wr < 30 and len(ts) >= 5:
                    weak_tiers.append(f"{t} ({wr:.0f}%)")

        weak_tier_note = ""
        if weak_tiers:
            weak_tier_note = (
                f"\n⚠️ <b>Tier Underperforming:</b> {', '.join(weak_tiers)}\n"
                f"<i>Pertimbangkan menaikkan threshold tier tersebut</i>\n"
            )

        # [G3-REPORT] Analytics base_score vs bonus_score — memanfaatkan data F1 (v3.6)
        # Hitung berapa sinyal per tier yang "naik" karena bonus S1/S2/S3
        bonus_note = ""
        try:
            res_bonus = (
                sb().table("gem_signals")
                .select("tier,base_score,bonus_score,outcome")
                .not_.is_("base_score", "null")
                .execute()
            )
            bonus_data = res_bonus.data or []
            if len(bonus_data) >= 5:
                # Sinyal yang tier-nya didapat dari base_score (bukan terbantu bonus)
                pure_base  = [s for s in bonus_data if s.get("base_score", 0) >= 26]  # pure MOONSHOT base
                bonus_push = [s for s in bonus_data
                              if s.get("base_score", 0) < 26
                              and (s.get("base_score", 0) or 0) + (s.get("bonus_score", 0) or 0) >= 26]
                avg_base  = sum(s.get("base_score", 0) or 0 for s in bonus_data) / len(bonus_data)
                avg_bonus = sum(s.get("bonus_score", 0) or 0 for s in bonus_data) / len(bonus_data)
                bonus_note = (
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"📐 <b>Score Analytics</b> (n={len(bonus_data)})\n"
                    f"Avg base: <b>{avg_base:.1f}/32</b>  |  Avg bonus: <b>{avg_bonus:.1f}/5</b>\n"
                    f"Pure base MOONSHOT (≥26 base): <b>{len(pure_base)}</b>\n"
                    f"Bonus-assisted MOONSHOT: <b>{len(bonus_push)}</b>\n"
                )
        except Exception as e:
            log(f"winrate bonus analytics: {e}", "warn")

        tg(
            f"📊 <b>GEM HUNTER — WIN RATE REPORT</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"Total  : <b>{total}</b>  |  Win: <b>{len(wins)}</b>  "
            f"|  Loss: <b>{len(losses)}</b>  |  Expired: <b>{len(expired)}</b>\n"
            f"Win Rate : <b>{win_rate:.1f}%</b>  |  Avg Win: <b>{avg_win:+.1f}%</b>  "
            f"|  Avg Loss: <b>{avg_loss:+.1f}%</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"TP1: {tp_hits['TP1']}  |  TP2: {tp_hits['TP2']}  |  TP3: {tp_hits['TP3']}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Per Tier:</b>\n<code>{tier_lines}</code>"
            f"{weak_tier_note}"
            f"{bonus_note}"
            f"<i>{datetime.now(WIB).strftime('%d/%m/%Y %H:%M WIB')}</i>"
        )
        log("Win rate report terkirim", "ok")

        # [KRITIS-2] Simpan dedup key agar run berikutnya di hari yang sama tidak kirim ulang
        try:
            sb().table("gem_dedup").insert({
                "pair": today_key,
                "sent_at": datetime.now(timezone.utc).isoformat(),
            }).execute()
        except Exception as e:
            log(f"winrate_report dedup insert: {e}", "warn")

    except Exception as e:
        log(f"winrate_report: {e}", "warn")


# ══════════════════════════════════════════════════════════════════
#  MAIN SCAN RUNNER
# ══════════════════════════════════════════════════════════════════

def load_signals_sent_today() -> int:
    """
    [FIX-D4] Hitung berapa sinyal sudah terkirim dalam 24 jam terakhir.
    Digunakan untuk membatasi total notifikasi agar tidak spam saat market volatile.
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        res = sb().table("gem_signals").select("id").gte("created_at", cutoff).execute()
        return len(res.data or [])
    except Exception as e:
        log(f"load_signals_sent_today: {e}", "warn")
        return 0


def run_scan() -> None:
    log("=" * 55)
    log(f"💎 GEM HUNTER v3.7 Audit Complete — {datetime.now(WIB).strftime('%Y-%m-%d %H:%M WIB')}")
    log("=" * 55)

    client = build_gate_client()

    log("Mengecek outcome signal sebelumnya...")
    updated = check_outcomes(client)
    if updated:
        log(f"  ✔ {updated} outcome diupdate", "ok")

    if datetime.now(WIB).hour == WINRATE_REPORT_HOUR:
        log("Mengirim win rate report harian...")
        send_winrate_report()

    # [G2-LOG] Log sesi aktif agar operator tahu mode scan saat ini
    _prime = is_prime_session()
    _vol_thr = get_vol_spike_threshold()
    _sleep   = get_scan_sleep()
    log(
        f"{'🔥 Prime Time' if _prime else '🌙 Off-Peak'} session "
        f"| Vol spike threshold: {_vol_thr}× "
        f"| Scan sleep: {_sleep}s/ticker"
    )

    # [FIX-B3] BTC regime dengan rolling lookback
    btc = get_btc_regime(client)
    log(f"BTC 6h(1h candle): {btc['btc_1h']:+.1f}%  |  BTC 16h(4h candle): {btc['btc_4h']:+.1f}%")

    if btc["halt"]:
        log("🛑 BTC crash — scan dilewati", "warn")
        tg(format_summary(0, 0, 0, 0, halted=True))
        return
    if btc["block_buy"]:
        log("⚠️ BTC drop — scan jalan, threshold ketat", "warn")

    log("Fetching tickers Gate.io...")
    tickers = gate_retry(client.list_tickers) or []
    log(f"Total tickers: {len(tickers)}")

    # [FIX-B4] Load dedup cache SEKALI — tidak query per pair
    dedup_cache = load_dedup_cache()

    # [UPG-2] Build ticker_map untuk sector rotation — zero extra API calls
    ticker_map: dict[str, float] = {}
    for t in tickers:
        p = getattr(t, "currency_pair", None)
        if p:
            _cp = getattr(t, "change_percentage", None)
            if _cp not in (None, "", "NaN"):
                try:
                    _f = float(_cp)
                    if not math.isnan(_f):
                        ticker_map[p] = _f
                except Exception:
                    pass
    log(f"Ticker map built: {len(ticker_map)} pairs dengan data change%")

    candidates: list[dict] = []
    scanned = skipped = 0
    gate_stats: dict = {}   # [DIAG] kumpulkan statistik penolakan per gate

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
            if not (GEM_VOL_MIN <= vol_24h <= GEM_VOL_MAX):
                skipped += 1
                continue
            if pair in dedup_cache:
                continue

            scanned += 1
            sig = analyze_gem(client, pair, price, vol_24h, change_24h,
                              ticker_map, btc_block_buy=btc["block_buy"],
                              gate_stats=gate_stats)
            if sig:
                candidates.append(sig)
                log(
                    f"  ✔ {pair} [{sig['tier']}] score={sig['score']}/37 "
                    f"vol×{sig['vol_ratio']:.1f} tf={sig['tf_alignment']}/3 "
                    f"depth={sig['depth_score']}/2 sector={sig['sector']}",
                    "ok",
                )

            time.sleep(get_scan_sleep())   # [D5-SCAN] adaptive: 0.20s prime, 0.40s off-peak

        except Exception as e:
            log(f"  [{pair}] {e}", "warn")

    log(f"\nScan selesai: {scanned} diperiksa | {skipped} diluar vol | {len(candidates)} kandidat")

    # [DIAG] Print gate rejection breakdown
    if gate_stats:
        log("─" * 50)
        log("📊 GATE REJECTION DIAGNOSTICS:")
        total_rejected = sum(gate_stats.values())
        sorted_gates = sorted(gate_stats.items(), key=lambda x: -x[1])
        for gate_name, count in sorted_gates[:15]:   # top 15 penyebab penolakan
            pct = count / scanned * 100 if scanned > 0 else 0
            bar = "█" * min(int(pct / 2), 30)
            log(f"  {gate_name:<40} {count:>4}× ({pct:>5.1f}%) {bar}")
        log(f"  {'TOTAL DITOLAK':<40} {total_rejected:>4}×")
        log("─" * 50)

    if not candidates:
        log("Tidak ada gem ditemukan.")
        tg(format_summary(scanned, skipped, 0, 0))
        return

    # Sort: tier → score → vol_ratio
    tier_order = {"MOONSHOT": 0, "GEM": 1, "WATCH": 2}
    candidates.sort(key=lambda x: (tier_order.get(x["tier"], 9), -x["score"], -x["vol_ratio"]))

    # [FIX-D4] Cek kuota harian sebelum mengirim
    signals_today = load_signals_sent_today()
    daily_quota_left = max(0, MAX_SIGNALS_PER_DAY - signals_today)
    if daily_quota_left == 0:
        log(f"⚠ Kuota harian tercapai ({MAX_SIGNALS_PER_DAY} sinyal/24h) — tidak mengirim", "warn")
        tg(format_summary(scanned, skipped, len(candidates), 0))
        return

    sent = 0
    max_this_run = min(MAX_SIGNALS_PER_RUN, daily_quota_left)
    for sig in candidates:
        if sent >= max_this_run:
            break
        tg(format_signal(sig))
        save_signal(sig)
        mark_sent(sig["pair"])
        # Update local cache agar pair yang baru terkirim juga terskip di loop ini
        dedup_cache.add(sig["pair"])
        log(f"  📤 {sig['pair']} [{sig['tier']}] score={sig['score']}/37", "ok")
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
        log("Interrupted.")
        sys.exit(0)
    except Exception as e:
        log(f"FATAL: {e}", "error")
        log(traceback.format_exc(), "error")
        try:
            tg(
                f"❌ <b>GEM HUNTER — ERROR</b>\n"
                f"<code>{str(e)[:300]}</code>\n"
                f"<i>{datetime.now(WIB).strftime('%d/%m/%Y %H:%M WIB')}</i>"
            )
        except Exception:
            pass
        sys.exit(1)
