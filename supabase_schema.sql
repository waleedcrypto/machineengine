-- ══════════════════════════════════════════════════════════════
--  MW TRADER — Supabase Schema
--  Run this in Supabase SQL Editor to create all tables
-- ══════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────
--  1. LATEST SIGNAL (single-row upsert, id=1)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS latest_signal (
    id                BIGINT        PRIMARY KEY DEFAULT 1,
    signal_id         TEXT,
    symbol            TEXT          NOT NULL DEFAULT 'BTCUSDT',
    timeframe         TEXT          NOT NULL DEFAULT '1m',
    direction         TEXT          NOT NULL DEFAULT 'NO_TRADE',
    status            TEXT          NOT NULL DEFAULT 'NO_TRADE',
    entry_low         NUMERIC(18,4),
    entry_high        NUMERIC(18,4),
    suggested_entry   NUMERIC(18,4),
    stop_loss         NUMERIC(18,4),
    tp1               NUMERIC(18,4),
    tp2               NUMERIC(18,4),
    tp3               NUMERIC(18,4),
    risk_reward_tp1   NUMERIC(6,2),
    risk_reward_tp2   NUMERIC(6,2),
    risk_reward_tp3   NUMERIC(6,2),
    tp1_probability   NUMERIC(6,2)  DEFAULT 0,
    tp2_probability   NUMERIC(6,2)  DEFAULT 0,
    tp3_probability   NUMERIC(6,2)  DEFAULT 0,
    sl_risk           NUMERIC(6,2)  DEFAULT 0,
    confidence        NUMERIC(6,2)  DEFAULT 0,
    signal_quality    TEXT          DEFAULT 'NO_TRADE',
    market_regime     TEXT          DEFAULT 'UNKNOWN',
    orderflow_label   TEXT          DEFAULT 'NO_DATA',
    structure_reason  TEXT,
    full_reason       TEXT,
    features_json     JSONB,
    current_price     NUMERIC(18,4),
    created_at        TIMESTAMPTZ   DEFAULT NOW(),
    updated_at        TIMESTAMPTZ   DEFAULT NOW(),
    expires_at        TIMESTAMPTZ
);

-- ─────────────────────────────────────────────
--  2. SIGNAL HISTORY
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS signal_history (
    id                BIGSERIAL     PRIMARY KEY,
    signal_id         TEXT,
    symbol            TEXT          NOT NULL DEFAULT 'BTCUSDT',
    timeframe         TEXT          NOT NULL DEFAULT '1m',
    direction         TEXT          NOT NULL,
    entry_low         NUMERIC(18,4),
    entry_high        NUMERIC(18,4),
    stop_loss         NUMERIC(18,4),
    tp1               NUMERIC(18,4),
    tp2               NUMERIC(18,4),
    tp3               NUMERIC(18,4),
    confidence        NUMERIC(6,2)  DEFAULT 0,
    tp1_probability   NUMERIC(6,2)  DEFAULT 0,
    tp2_probability   NUMERIC(6,2)  DEFAULT 0,
    tp3_probability   NUMERIC(6,2)  DEFAULT 0,
    sl_risk           NUMERIC(6,2)  DEFAULT 0,
    signal_quality    TEXT          DEFAULT 'C',
    market_regime     TEXT,
    orderflow_label   TEXT,
    result            TEXT          DEFAULT 'OPEN',
    hit_level         TEXT          DEFAULT 'NONE',
    created_at        TIMESTAMPTZ   DEFAULT NOW(),
    closed_at         TIMESTAMPTZ,
    full_reason       TEXT,
    features_json     JSONB
);

CREATE INDEX IF NOT EXISTS idx_signal_history_created ON signal_history(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_history_direction ON signal_history(direction);
CREATE INDEX IF NOT EXISTS idx_signal_history_result ON signal_history(result);
CREATE INDEX IF NOT EXISTS idx_signal_history_timeframe ON signal_history(timeframe);

-- ─────────────────────────────────────────────
--  3. ENGINE STATUS (single-row upsert, id=1)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS engine_status (
    id                BIGINT        PRIMARY KEY DEFAULT 1,
    symbol            TEXT          NOT NULL DEFAULT 'BTCUSDT',
    timeframe         TEXT          NOT NULL DEFAULT '1m',
    status            TEXT          NOT NULL DEFAULT 'STOPPED',
    websocket_status  TEXT          DEFAULT 'DISCONNECTED',
    last_price        NUMERIC(18,4) DEFAULT 0,
    last_candle_time  TIMESTAMPTZ,
    last_signal_time  TIMESTAMPTZ,
    total_signals     INTEGER       DEFAULT 0,
    wins              INTEGER       DEFAULT 0,
    losses            INTEGER       DEFAULT 0,
    win_rate          NUMERIC(6,2)  DEFAULT 0,
    updated_at        TIMESTAMPTZ   DEFAULT NOW()
);

-- Insert default row
INSERT INTO engine_status (id, status)
VALUES (1, 'STOPPED')
ON CONFLICT (id) DO NOTHING;

-- ─────────────────────────────────────────────
--  4. MARKET SNAPSHOTS (optional enrichment)
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_snapshots (
    id                BIGSERIAL     PRIMARY KEY,
    symbol            TEXT          NOT NULL DEFAULT 'BTCUSDT',
    timeframe         TEXT          NOT NULL DEFAULT '1m',
    snapshot_time     TIMESTAMPTZ   DEFAULT NOW(),
    price             NUMERIC(18,4),
    atr               NUMERIC(18,6),
    market_regime     TEXT,
    orderflow_30s     TEXT,
    orderflow_1m      TEXT,
    orderflow_3m      TEXT,
    orderflow_5m      TEXT,
    buy_vol_1m        NUMERIC(18,4),
    sell_vol_1m       NUMERIC(18,4),
    delta_1m          NUMERIC(18,4),
    swing_high        NUMERIC(18,4),
    swing_low         NUMERIC(18,4),
    created_at        TIMESTAMPTZ   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_snapshots_time ON market_snapshots(snapshot_time DESC);

-- ─────────────────────────────────────────────
--  5. MODEL STATS
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS model_stats (
    id                BIGSERIAL     PRIMARY KEY,
    timeframe         TEXT          NOT NULL DEFAULT '1m',
    total_patterns    INTEGER       DEFAULT 0,
    total_signals     INTEGER       DEFAULT 0,
    wins              INTEGER       DEFAULT 0,
    losses            INTEGER       DEFAULT 0,
    win_rate          NUMERIC(6,2)  DEFAULT 0,
    avg_confidence    NUMERIC(6,2)  DEFAULT 0,
    updated_at        TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE(timeframe)
);

-- Insert default row
INSERT INTO model_stats (timeframe)
VALUES ('1m')
ON CONFLICT (timeframe) DO NOTHING;

-- ─────────────────────────────────────────────
--  ROW-LEVEL SECURITY — Dashboard anon read
-- ─────────────────────────────────────────────
ALTER TABLE latest_signal    ENABLE ROW LEVEL SECURITY;
ALTER TABLE signal_history   ENABLE ROW LEVEL SECURITY;
ALTER TABLE engine_status    ENABLE ROW LEVEL SECURITY;
ALTER TABLE market_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_stats      ENABLE ROW LEVEL SECURITY;

-- Allow anon read on all tables (dashboard read-only)

DROP POLICY IF EXISTS "anon_read_latest_signal" ON latest_signal;
CREATE POLICY "anon_read_latest_signal"
    ON latest_signal FOR SELECT TO anon USING (true);

DROP POLICY IF EXISTS "anon_read_signal_history" ON signal_history;
CREATE POLICY "anon_read_signal_history"
    ON signal_history FOR SELECT TO anon USING (true);

DROP POLICY IF EXISTS "anon_read_engine_status" ON engine_status;
CREATE POLICY "anon_read_engine_status"
    ON engine_status FOR SELECT TO anon USING (true);

DROP POLICY IF EXISTS "anon_read_market_snapshots" ON market_snapshots;
CREATE POLICY "anon_read_market_snapshots"
    ON market_snapshots FOR SELECT TO anon USING (true);

DROP POLICY IF EXISTS "anon_read_model_stats" ON model_stats;
CREATE POLICY "anon_read_model_stats"
    ON model_stats FOR SELECT TO anon USING (true);

-- Service role can write (machine.py uses service role key)
-- Service role bypasses RLS by default in Supabase.