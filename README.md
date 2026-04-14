# 💎 GEM HUNTER BOT

Bot signal otomatis untuk mendeteksi koin berpotensi **50x–100x** di Gate.io.

**Stack:** Python · Gate.io API · Supabase · Telegram · GitHub Actions

---

## Filosofi

Bot ini mendeteksi **dormancy break** — koin yang sudah flat/sideways selama 10+ hari tiba-tiba
menunjukkan tanda kebangkitan: lonjakan volume 8x+ dan awal pergerakan harga dari base.

Tangkap **sebelum** pump besar — bukan sesudah.

---

## Kriteria Signal

| Gate | Parameter | Nilai |
|------|-----------|-------|
| Vol 24h | Zona ultra-low cap | 500 – 30.000 USDT |
| Flat base | Periode dormant | 10 hari (60 candle 4h) |
| Avg range flat | Seberapa dormant | < 3% per candle |
| Vol spike | Awakening | ≥ 8× rata-rata flat |
| Breakout | Awal pergerakan | ≥ 4% dari base |
| RSI | Ruang naik | 32 – 65 |
| ATH distance | Recovery potential | ≥ 30% di bawah ATH |
| Dist from base | Belum terlambat | ≤ 50% dari base |

**Tiers:** `WATCH` (score ≥7) · `GEM` (≥9) · `MOONSHOT` (≥13)

---

## Setup

### 1. Supabase

1. Buat project baru di [supabase.com](https://supabase.com)
2. Buka **SQL Editor**, jalankan isi file `supabase/schema.sql`
3. Catat **Project URL** dan **service_role key** dari Settings → API

### 2. Telegram Bot

1. Chat [@BotFather](https://t.me/BotFather) → `/newbot`
2. Catat **Bot Token**
3. Kirim pesan ke bot kamu, lalu buka:
   `https://api.telegram.org/bot<TOKEN>/getUpdates`
4. Catat **chat_id** dari response

### 3. Gate.io API Key

1. Login Gate.io → Account → API Management
2. Buat API key dengan permission **Read Only** (tidak perlu trading)
3. Catat Key dan Secret

### 4. GitHub Repository

1. Fork / upload folder ini ke GitHub
2. Buka **Settings → Secrets and variables → Actions**
3. Tambahkan secrets berikut:

| Secret | Isi |
|--------|-----|
| `GATE_API_KEY` | Gate.io API Key |
| `GATE_API_SECRET` | Gate.io API Secret |
| `TG_BOT_TOKEN` | Telegram Bot Token |
| `TG_CHAT_ID` | Telegram Chat ID (bisa group/channel/personal) |
| `SUPABASE_URL` | `https://xxxx.supabase.co` |
| `SUPABASE_KEY` | service_role key dari Supabase |

### 5. Aktifkan GitHub Actions

1. Buka tab **Actions** di repo
2. Klik **Enable workflows** jika diminta
3. Bot akan jalan otomatis setiap 6 jam (00:00 / 06:00 / 12:00 / 18:00 UTC)
4. Untuk run manual: Actions → **Gem Hunter Bot** → **Run workflow**

---

## Struktur File

```
gem_hunter_bot/
├── gem_hunter_bot.py          # Bot utama
├── requirements.txt           # Python dependencies
├── supabase/
│   └── schema.sql             # DDL tabel Supabase (jalankan sekali)
└── .github/
    └── workflows/
        └── gem_hunter.yml     # GitHub Actions schedule
```

---

## Jadwal Default

```
Cron: 0 */6 * * *
```

| UTC   | WIB   |
|-------|-------|
| 00:00 | 07:00 |
| 06:00 | 13:00 |
| 12:00 | 19:00 |
| 18:00 | 01:00 |

Ubah jadwal di `.github/workflows/gem_hunter.yml` sesuai preferensi.

---

## Format Signal Telegram

```
🚀 GEM HUNTER — MOONSHOT  🟢 BUY
────────────────────────
Pair    : XYZ/USDT  [4h]
⏰ Valid : 15/01 13:00 → 17/01 13:00 WIB
────────────────────────
Entry   : $0.00012345  ≈ Rp 1.914
TP1     : $0.00018517  ≈ Rp 2.872  (+50%)
TP2     : $0.00030862  ≈ Rp 4.789  (+150%)
TP3     : $0.00074070  (+500% moonshot)
SL      : $0.00011357  ≈ Rp 1.761  (-8.0%)
R/R     : 1:6.2
────────────────────────
📊 Vol spike    : 18.4× rata-rata flat
📈 Breakout     : +7.2% dari base
💤 Dormancy     : 0.7% avg range
📉 ATH dist     : 82% di bawah ATH historis
RSI             : 41.5  |  24h: +9.1%
Score           : 14  (Dormancy 3/3 · Vol 2/3 · Breakout 2/3)
────────────────────────
🧲 Liquidity sweep terdeteksi — smart money sudah masuk
😴 Deep sleep (0.7% avg range) — energi tersimpan panjang
```

---

## Risk Warning

> ⚠️ Setup ini adalah **extreme high risk**.
> - Alokasi maksimal **0.5–1% modal** per posisi
> - Hold time **1–7 hari** — bukan untuk scalping
> - **SL wajib** dipasang — koin kecil bisa dump 50%+ dalam jam
> - Bukan rekomendasi finansial

---

## Tracking Performance

Setelah signal masuk ke Supabase, jalankan query ini untuk lihat win rate:

```sql
SELECT * FROM gem_performance;
```

Update outcome signal setelah TP/SL hit:

```sql
UPDATE gem_signals
SET outcome = 'WIN_TP1', outcome_pct = 50.0, outcome_at = NOW()
WHERE pair = 'XYZ_USDT' AND id = 42;
```
