-- ─── PCI schema — DROP + RECREATE FROM SCRATCH ───────────────────────────────
-- WARNING: this permanently deletes all rows in frames/segments/violations.
-- Confirmed intentional — re-run pci_api_upload.py afterward to reload frames.

USE pci;

SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS violations;
DROP TABLE IF EXISTS segments;
DROP TABLE IF EXISTS frames;

SET FOREIGN_KEY_CHECKS = 1;

-- ── frames ────────────────────────────────────────────────────────────────────
CREATE TABLE frames (
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

-- ── segments ──────────────────────────────────────────────────────────────────
CREATE TABLE segments (
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
    length_meters     FLOAT         DEFAULT 0,
    created_at        VARCHAR(50),
    sealed_at         VARCHAR(50),
    health_score      INT           DEFAULT NULL,
    health_condition  VARCHAR(20)   DEFAULT NULL,
    health_color      VARCHAR(10)   DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── violations ────────────────────────────────────────────────────────────────
CREATE TABLE violations (
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
    length_px             FLOAT,
    breadth_px            FLOAT,
    bbox_area_px          FLOAT,
    polygon_area_px       FLOAT,
    gsd_mm_per_px         FLOAT,
    image_width           INT,
    image_height          INT,
    latitude              DOUBLE,
    longitude             DOUBLE,
    annotated_image_path  VARCHAR(512),
    created_at            VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX idx_seg_active_end  ON segments  (status, end_lat, end_lon);
CREATE INDEX idx_viol_frame_id   ON violations (frame_id);
CREATE INDEX idx_viol_segment_id ON violations (segment_id);
CREATE INDEX idx_frame_status    ON frames     (status);
