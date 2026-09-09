-- ─── MBS Corridor Schema — Makkah↔Jeddah frozen-segment feature ───────────────
-- Idempotent: safe to run repeatedly against the live DB. Additive only —
-- never touches frames / segments / violations. This file is the single
-- source of truth for this feature's schema; any future column addition
-- must be reflected here (not only via a runtime ALTER), to avoid repeating
-- the schema-drift problem this feature exists to fix (segments.segment_name,
-- road_type, segment_width_m exist on the live DB but were never captured in
-- any committed .sql file — see db/init.sql vs db/pci_schema_recreate.sql).

USE pci;

-- ── mbs_segments — one-time frozen snapshot of corridor segment geometry ──────
-- Populated once by freeze_mbs_segments.py. Never rewritten after creation
-- except to add newly-discovered frozen segments if the corridor extends.
CREATE TABLE IF NOT EXISTS mbs_segments (
    mbs_segment_id    VARCHAR(36)   PRIMARY KEY,
    source_segment_id VARCHAR(36)   DEFAULT NULL,   -- live segments.segment_id at freeze time
                                                       -- (audit only — NOT joinable later, since
                                                       -- offline_resegment.py regenerates live IDs)
    start_lat         DOUBLE        NOT NULL,
    start_lon         DOUBLE        NOT NULL,
    end_lat           DOUBLE,
    end_lon           DOUBLE,
    gps_path          JSON          NOT NULL,         -- full-density raw path, frozen verbatim
    length_meters     FLOAT         DEFAULT 0,
    municipality      VARCHAR(255),
    submunicipality   VARCHAR(255),
    segment_name      VARCHAR(255),
    road_type         VARCHAR(50)   DEFAULT NULL,
    segment_width_m   FLOAT         DEFAULT NULL,
    frozen_at         DATETIME      DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── mbs_score_thresholds — frozen percentile baseline ──────────────────────────
-- Populated once via map_match_mbs_frames.py --init-thresholds. Never
-- auto-recomputed — this is what makes health_score comparable across dates.
-- A --recompute-thresholds run only affects scores computed AFTER that point;
-- existing mbs_segment_scores rows keep whatever thresholds were active when
-- they were computed.
CREATE TABLE IF NOT EXISTS mbs_score_thresholds (
    metric               VARCHAR(20)  PRIMARY KEY,   -- 'pothole' | 'alligator' | 'longitudinal'
    p25                  FLOAT        NOT NULL,
    p50                  FLOAT        NOT NULL,
    p75                  FLOAT        NOT NULL,
    baseline_frame_count INT          NOT NULL,       -- how many frames fed the baseline calc
    baseline_note        VARCHAR(255) DEFAULT NULL,   -- e.g. "Jan-Mar 2026 + Jul 17 backfill"
    computed_at          DATETIME     DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── mbs_segment_scores — append-only per (segment, capture_date) history ──────
CREATE TABLE IF NOT EXISTS mbs_segment_scores (
    id                        INT AUTO_INCREMENT PRIMARY KEY,
    mbs_segment_id            VARCHAR(36)  NOT NULL,
    capture_date              DATE         NOT NULL,
    frame_count               INT          DEFAULT 0,
    violation_count           INT          DEFAULT 0,
    pothole_count             INT          DEFAULT 0,
    alligator_count           INT          DEFAULT 0,
    longitudinal_count        INT          DEFAULT 0,
    pothole_pixel_area        FLOAT        DEFAULT 0,
    alligator_pixel_area      FLOAT        DEFAULT 0,
    longitudinal_pixel_area   FLOAT        DEFAULT 0,
    pothole_px_km             FLOAT        DEFAULT 0,
    alligator_px_km           FLOAT        DEFAULT 0,
    longitudinal_px_km        FLOAT        DEFAULT 0,
    health_score              INT          DEFAULT NULL,
    health_condition          VARCHAR(20)  DEFAULT NULL,
    health_color              VARCHAR(10)  DEFAULT NULL,
    computed_at               DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_mbs_seg_date (mbs_segment_id, capture_date),
    KEY idx_mbs_scores_date (capture_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── mbs_frame_segment_map — map-matching output, one row per frame ────────────
-- Frames evaluated but rejected (outside tolerance) are still recorded with
-- mbs_segment_id=NULL / match_status='unmatched' so reruns don't re-evaluate
-- them forever and corridor-boundary drift stays visible over time.
CREATE TABLE IF NOT EXISTS mbs_frame_segment_map (
    frame_id       VARCHAR(255) PRIMARY KEY,
    mbs_segment_id VARCHAR(36)  DEFAULT NULL,
    capture_date   DATE         NOT NULL,
    distance_m     FLOAT        DEFAULT NULL,
    match_status   VARCHAR(20)  NOT NULL DEFAULT 'matched',  -- 'matched' | 'unmatched'
    matched_at     DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_mbs_map_segment_date (mbs_segment_id, capture_date),
    KEY idx_mbs_map_date (capture_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── mbs_road_geometry — OSM reference centerline, display-only ────────────────
-- Optional / audit copy. The primary serving path is a static file written by
-- fetch_mbs_road_geometry.py to app/static/mbs_road_geometry.geojson; this
-- table just keeps a DB-side record of what was fetched and when.
CREATE TABLE IF NOT EXISTS mbs_road_geometry (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    source          VARCHAR(50)  DEFAULT 'overpass',
    osm_relation_id BIGINT       DEFAULT NULL,
    geojson         JSON         NOT NULL,
    fetched_at      DATETIME     DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
