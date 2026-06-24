-- -- ─── PCI Database Schema ──────────────────────────────────────────────────────
-- CREATE DATABASE IF NOT EXISTS pci;
-- USE pci;
 
-- CREATE TABLE IF NOT EXISTS frames (
--     frame_id          VARCHAR(255)  PRIMARY KEY,
--     latitude          DOUBLE        NOT NULL,
--     longitude         DOUBLE        NOT NULL,
--     image_key         VARCHAR(512)  NOT NULL,
--     image_url         TEXT,
--     image_bucket      VARCHAR(255)  NOT NULL,
--     municipality      VARCHAR(255),
--     submunicipality   VARCHAR(255),
--     datetime_utc      VARCHAR(50),
--     local_image_path  VARCHAR(512),
--     image_metadata    JSON,
--     status            VARCHAR(50)   DEFAULT 'pending',
--     created_at        VARCHAR(50),
--     updated_at        VARCHAR(50)
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
 


 -- ─── PCI Database Schema ──────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS pci;
USE pci;

-- ── frames ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS frames (
    frame_id          VARCHAR(255)  PRIMARY KEY,
    latitude          DOUBLE        NOT NULL,
    longitude         DOUBLE        NOT NULL,
    image_key         VARCHAR(512)  NOT NULL,
    image_url         TEXT,
    image_bucket      VARCHAR(255)  NOT NULL,
    municipality      VARCHAR(255),
    submunicipality   VARCHAR(255),
    datetime_utc      VARCHAR(50),
    local_image_path  VARCHAR(512),
    image_metadata    JSON,
    status            VARCHAR(50)   DEFAULT 'pending',
    pci_score         FLOAT         DEFAULT NULL,
    pci_rating        VARCHAR(20)   DEFAULT NULL,
    created_at        VARCHAR(50),
    updated_at        VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── segments (straight road segments) ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS segments (
    segment_id        VARCHAR(36)   PRIMARY KEY,
    start_lat         DOUBLE        NOT NULL,
    start_lon         DOUBLE        NOT NULL,
    end_lat           DOUBLE,
    end_lon           DOUBLE,
    gps_path          JSON,
    frame_count       INT           DEFAULT 0,
    violation_count   INT           DEFAULT 0,
    municipality      VARCHAR(255),
    submunicipality   VARCHAR(255),
    status            VARCHAR(50)   DEFAULT 'active',
    pci_score         FLOAT         DEFAULT NULL,
    pci_rating        VARCHAR(20)   DEFAULT NULL,
    created_at        VARCHAR(50),
    sealed_at         VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── violations (one row per detected defect) ──────────────────────────────────
CREATE TABLE IF NOT EXISTS violations (
    id                    INT AUTO_INCREMENT PRIMARY KEY,
    frame_id              VARCHAR(255)  NOT NULL,
    segment_id            VARCHAR(36),
    label                 VARCHAR(100)  NOT NULL,
    confidence            FLOAT         NOT NULL,
    severity              VARCHAR(50)   DEFAULT 'low',
    bbox_xmin             FLOAT,
    bbox_ymin             FLOAT,
    bbox_xmax             FLOAT,
    bbox_ymax             FLOAT,
    polygon_points        JSON,
    length_mm             FLOAT,
    breadth_mm            FLOAT,
    bbox_area_mm2         FLOAT,
    polygon_area_mm2      FLOAT,
    gsd_mm_per_px         FLOAT,
    image_width           INT,
    image_height          INT,
    latitude              DOUBLE,
    longitude             DOUBLE,
    annotated_image_path  VARCHAR(512),
    created_at            VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Indexes for 50k+ scale ────────────────────────────────────────────────────
-- Speeds up segment bounding-box lookup + FOR UPDATE in process_segment
CREATE INDEX idx_seg_active_end  ON segments  (status, end_lat, end_lon);
-- Speeds up violation JOINs and dashboard aggregation queries
CREATE INDEX idx_viol_frame_id   ON violations (frame_id);
CREATE INDEX idx_viol_segment_id ON violations (segment_id);
-- Speeds up pipeline status count queries
CREATE INDEX idx_frame_status    ON frames     (status);