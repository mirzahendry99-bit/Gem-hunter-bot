-- ══════════════════════════════════════════════════════════════
--  GEM HUNTER — Supabase Schema
--  Jalankan SQL ini di Supabase SQL Editor (satu kali setup)
-- ══════════════════════════════════════════════════════════════


-- ── Tabel 1: gem_signals ──────────────────────────────────────
-- Menyimpan semua signal yang dikirim ke Telegram.
-- Digunakan untuk history, backtesting, dan performance tracking.

CREATE TABLE IF NOT EXISTS gem_signals (
  id              BIGSERIAL PRIMARY KEY,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- Identitas signal
  pair            TEXT        NOT NULL,
  tier            TEXT        NOT NULL CHECK (tier IN ('MOONSHOT', 'GEM', 'WATCH')),
  score           INTEGER     NOT NULL,

  -- Entry / TP / SL
  entry           NUMERIC(20, 8) NOT NULL,
  tp1             NUMERIC(20, 8) NOT NULL,
  tp2             NUMERIC(20, 8) NOT NULL,
  tp3             NUMERIC(20, 8) NOT NULL,
  sl              NUMERIC(20, 8) NOT NULL,
  rr              NUMERIC(6, 2)  NOT NULL,

  -- Indikator teknikal saat signal
  rsi             NUMERIC(6, 2),
  vol_ratio       NUMERIC(8, 2),
  breakout_pct    NUMERIC(8, 2),
  avg_flat_range  NUMERIC(8, 2),
  ath_dist_pct    NUMERIC(8, 2),
  dist_from_base  NUMERIC(8, 2),
  change_24h      NUMERIC(8, 2),

  -- Konteks tambahan
  is_new_listing  BOOLEAN DEFAULT FALSE,
  has_sweep       BOOLEAN DEFAULT FALSE,
  macd_bull       BOOLEAN DEFAULT FALSE,

  -- Outcome tracking (diisi manual atau via update job nanti)
  outcome         TEXT        CHECK (outcome IN ('WIN_TP1', 'WIN_TP2', 'WIN_TP3', 'LOSS_SL', 'EXPIRED', NULL)),
  outcome_pct     NUMERIC(8, 2),
  outcome_at      TIMESTAMPTZ
);

-- Index untuk query cepat
CREATE INDEX IF NOT EXISTS idx_gem_signals_pair       ON gem_signals (pair);
CREATE INDEX IF NOT EXISTS idx_gem_signals_created_at ON gem_signals (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_gem_signals_tier       ON gem_signals (tier);


-- ── Tabel 2: gem_dedup ────────────────────────────────────────
-- Menyimpan pair yang sudah dikirim signal-nya.
-- Bot cek tabel ini sebelum kirim — hindari spam signal sama.

CREATE TABLE IF NOT EXISTS gem_dedup (
  id        BIGSERIAL PRIMARY KEY,
  pair      TEXT        NOT NULL,
  sent_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gem_dedup_pair    ON gem_dedup (pair);
CREATE INDEX IF NOT EXISTS idx_gem_dedup_sent_at ON gem_dedup (sent_at DESC);


-- ── Auto-cleanup: hapus dedup entry lebih dari 7 hari ─────────
-- Jalankan ini sebagai scheduled job di Supabase (opsional)
-- atau bisa dibuat pg_cron job:

-- SELECT cron.schedule(
--   'cleanup-gem-dedup',
--   '0 0 * * *',  -- setiap tengah malam UTC
--   $$
--     DELETE FROM gem_dedup
--     WHERE sent_at < NOW() - INTERVAL '7 days';
--   $$
-- );


-- ── View: signal performance summary ─────────────────────────
-- Berguna untuk lihat win rate per tier

CREATE OR REPLACE VIEW gem_performance AS
SELECT
  tier,
  COUNT(*)                                            AS total_signals,
  COUNT(*) FILTER (WHERE outcome LIKE 'WIN%')         AS wins,
  COUNT(*) FILTER (WHERE outcome = 'LOSS_SL')         AS losses,
  COUNT(*) FILTER (WHERE outcome IS NULL)             AS pending,
  ROUND(
    COUNT(*) FILTER (WHERE outcome LIKE 'WIN%')::NUMERIC
    / NULLIF(COUNT(*) FILTER (WHERE outcome IS NOT NULL), 0) * 100,
    1
  )                                                   AS win_rate_pct,
  ROUND(AVG(outcome_pct) FILTER (WHERE outcome LIKE 'WIN%'), 1) AS avg_win_pct,
  ROUND(AVG(outcome_pct) FILTER (WHERE outcome = 'LOSS_SL'), 1) AS avg_loss_pct
FROM gem_signals
GROUP BY tier
ORDER BY
  CASE tier
    WHEN 'MOONSHOT' THEN 1
    WHEN 'GEM'      THEN 2
    WHEN 'WATCH'    THEN 3
  END;


-- ── Row Level Security (RLS) ──────────────────────────────────
-- Bot menggunakan service_role key (bypass RLS).
-- Aktifkan RLS untuk keamanan jika ada akses dari frontend.

ALTER TABLE gem_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE gem_dedup   ENABLE ROW LEVEL SECURITY;

-- Izinkan service_role (bot) full access — sudah otomatis bypass RLS.
-- Untuk anon/authenticated access, buat policy sesuai kebutuhan.
