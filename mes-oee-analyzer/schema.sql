-- MES OEE Analyzer — Supabase Schema
-- ========================================
-- Run this in the Supabase SQL Editor (Dashboard > SQL Editor > New Query)
--
-- Three tables:
--   equipment          — equipment knowledge base (machines, failure modes, fixes)
--   downtime_baselines — auto-computed duration averages per cause
--   run_history        — persistent run log (replaces ephemeral JSONL)

-- =========================================================================
-- 1. Equipment Knowledge Base
-- =========================================================================
CREATE TABLE IF NOT EXISTS equipment (
    id BIGSERIAL PRIMARY KEY,
    line TEXT NOT NULL DEFAULT 'Line 2 Flex',
    machine TEXT NOT NULL,
    failure_mode TEXT,
    cause_keywords TEXT[] NOT NULL DEFAULT '{}',
    expected_repair_hrs_min REAL,
    expected_repair_hrs_max REAL,
    common_fixes TEXT[] DEFAULT '{}',
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_equipment_machine ON equipment (machine);
CREATE INDEX IF NOT EXISTS idx_equipment_line ON equipment (line);

-- =========================================================================
-- 2. Downtime Baselines (auto-updated by tend_garden)
-- =========================================================================
CREATE TABLE IF NOT EXISTS downtime_baselines (
    id BIGSERIAL PRIMARY KEY,
    cause TEXT NOT NULL UNIQUE,
    avg_minutes REAL NOT NULL DEFAULT 0,
    std_minutes REAL DEFAULT 0,
    min_minutes REAL DEFAULT 0,
    max_minutes REAL DEFAULT 0,
    n_events INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_baselines_cause ON downtime_baselines (cause);

-- =========================================================================
-- 3. Run History (replaces history.jsonl for persistence)
-- =========================================================================
CREATE TABLE IF NOT EXISTS run_history (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    date_from DATE NOT NULL,
    date_to DATE NOT NULL,
    n_days INTEGER DEFAULT 1,
    avg_oee REAL,
    avg_availability REAL,
    avg_performance REAL,
    avg_quality REAL,
    utilization REAL,
    avg_cph REAL,
    total_cases REAL,
    total_hours REAL,
    cases_lost REAL,
    shifts JSONB DEFAULT '[]',
    top_downtime JSONB DEFAULT '[]',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_run_history_dates ON run_history (date_from, date_to);

-- =========================================================================
-- Seed Data — Line 2 Flex Equipment (Rochelle Plant)
-- =========================================================================
INSERT INTO equipment (line, machine, failure_mode, cause_keywords, expected_repair_hrs_min, expected_repair_hrs_max, common_fixes, notes)
VALUES
    -- Caser-Riverwood: #1 loss driver ($1.66M)
    ('Line 2 Flex', 'Caser-Riverwood', 'Glue System',
     ARRAY['caser', 'riverwood', 'glue', 'nordson', 'adhesive'],
     1.5, 2.0,
     ARRAY['Check nozzle wear', 'Verify temp settings', 'Check adhesive lot', 'Inspect glue pattern'],
     'Nordson glue system. #1 loss driver — $1.66M annual loss.'),

    ('Line 2 Flex', 'Caser-Riverwood', 'Fiber Jam',
     ARRAY['caser', 'riverwood', 'fiber jam', 'fiber mispick', 'fiber getting caught'],
     0.25, 0.5,
     ARRAY['Clear jam', 'Check fiber alignment', 'Inspect magazine feed'],
     NULL),

    ('Line 2 Flex', 'Caser-Riverwood', 'Case Forming',
     ARRAY['caser', 'riverwood', 'misformed case', 'misshapped case', 'ripping cases', 'open flaps', 'plastic drive bar'],
     0.25, 0.75,
     ARRAY['Check forming die alignment', 'Inspect plastic drive bar', 'Verify case blank dimensions'],
     NULL),

    -- Bear Labeler
    ('Line 2 Flex', 'Bear Labeler', 'Label Application',
     ARRAY['labeler', 'bear labeler', 'label', 'flappers', 'shiners', 'shinner', 'loose labels', 'ripped labels', 'curling bar', 'label fingers', 'labels weren''t sticking'],
     0.25, 0.75,
     ARRAY['Check curling bar setting', 'Verify adhesive temp', 'Inspect label fingers', 'Check label alignment'],
     NULL),

    -- Tray Packer - Kayat
    ('Line 2 Flex', 'Tray Packer - Kayat', NULL,
     ARRAY['tray packer', 'kayat', 'tray'],
     0.5, 1.0,
     ARRAY['Check tray feed', 'Inspect forming section', 'Verify tray blank size'],
     NULL),

    -- Shrink Tunnel - Kayat
    ('Line 2 Flex', 'Shrink Tunnel - Kayat', NULL,
     ARRAY['shrink tunnel', 'shrink wrapper', 'double-wrapped', 'shrink'],
     0.25, 0.5,
     ARRAY['Check tunnel temperature', 'Inspect conveyor speed', 'Verify film tension'],
     NULL),

    -- Palletizer-PAI
    ('Line 2 Flex', 'Palletizer-PAI', NULL,
     ARRAY['palletizer', 'pai', 'misformed layers', 'misshapped layers', 'misshappen', 'pallet conveyor'],
     0.5, 1.5,
     ARRAY['Check layer pattern', 'Inspect sweep mechanism', 'Verify pallet position sensor'],
     NULL),

    -- Conveyors
    ('Line 2 Flex', 'Conveyors', NULL,
     ARRAY['conveyor', 'conveyers', 'overhead conveyor', 'accumulation table', 'overhead conveypr'],
     0.25, 0.5,
     ARRAY['Check belt tension', 'Inspect sensors', 'Clear blockage'],
     NULL),

    -- Depal
    ('Line 2 Flex', 'Depal', NULL,
     ARRAY['depal', 'suction cup'],
     0.25, 0.75,
     ARRAY['Check suction cups', 'Inspect vacuum system', 'Verify can orientation'],
     NULL),

    -- Spiral/Ryson
    ('Line 2 Flex', 'Spiral/Ryson', NULL,
     ARRAY['ryson', 'spiral'],
     0.25, 0.5,
     ARRAY['Check chain tension', 'Inspect slats', 'Clear jam'],
     NULL),

    -- Printer - Diagraph
    ('Line 2 Flex', 'Printer - Diagraph', NULL,
     ARRAY['diagraph', 'printer', 'print and apply', 'laser', 'no print', 'laser jet', 'laser printer'],
     0.25, 0.5,
     ARRAY['Check printhead', 'Verify ink/ribbon', 'Clean sensors', 'Restart print controller'],
     NULL),

    -- Double Stacker
    ('Line 2 Flex', 'Double Stacker', NULL,
     ARRAY['stacker', 'double stacker', 'case stacker'],
     0.25, 0.5,
     ARRAY['Check sensor gap', 'Inspect pusher mechanism', 'Verify case alignment'],
     NULL),

    -- X-Ray
    ('Line 2 Flex', 'X-Ray', NULL,
     ARRAY['x-ray', 'x ray'],
     0.25, 0.5,
     ARRAY['Recalibrate detector', 'Check conveyor speed', 'Verify sensitivity settings'],
     NULL)

ON CONFLICT DO NOTHING;
