#!/usr/bin/env python3
"""
Export segment-level defect statistics to CSV.

Each row = one segment.
Pixel area = sum of (bbox_xmax - bbox_xmin) * (bbox_ymax - bbox_ymin)
             across all violations of that type in the segment.

Usage:
  python3 export_segment_report.py              # top 100 segments by frame count
  python3 export_segment_report.py --all        # all segments
  python3 export_segment_report.py --limit 500  # top N segments
"""

import argparse
import csv
import sys
import mysql.connector

DB = dict(host="127.0.0.1", port=3306,
          user="pci_user", password="pci_pass", database="pci")

FIELDNAMES = [
    "id",
    "Segment_name",
    "Total_frames_in_segment",
    "Distance_covered_km",
    "Pothole_count",
    "Pothole_total_area_pixels (all detections combined)",
    "Alligator_crack_count",
    "Alligator_crack_total_area_pixels (all detections combined)",
    "Longitudinal_crack_count",
    "Longitudinal_crack_total_area_pixels (all detections combined)",
]

DEFECT_LABELS = ("pothole", "alligator_crack", "longitudinal_crack")


def bbox_area_expr():
    """SQL expression: pixel area of one bbox row (clamped to 0)."""
    return "GREATEST(0, (bbox_xmax - bbox_xmin) * (bbox_ymax - bbox_ymin))"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all",   action="store_true", help="Export all segments")
    parser.add_argument("--limit", type=int, default=100,
                        help="Number of segments to export (default 100)")
    parser.add_argument("--out",   default="segment_report.csv",
                        help="Output CSV filename")
    args = parser.parse_args()

    limit_clause = "" if args.all else f"LIMIT {args.limit}"

    conn = mysql.connector.connect(**DB)
    cur  = conn.cursor(dictionary=True)

    print("Fetching segments…")
    cur.execute(f"""
        SELECT segment_id, segment_name, frame_count, length_meters
        FROM   segments
        ORDER  BY frame_count DESC
        {limit_clause}
    """)
    segments = cur.fetchall()
    print(f"  {len(segments)} segments loaded.")

    if not segments:
        print("No segments found. Exiting.")
        sys.exit(0)

    # Pull violation aggregates for all target segments in ONE query
    seg_ids = [s["segment_id"] for s in segments]
    fmt     = ",".join(["%s"] * len(seg_ids))

    print("Aggregating violations…")
    cur.execute(f"""
        SELECT
            segment_id,
            label,
            COUNT(*)                        AS cnt,
            SUM({bbox_area_expr()})         AS pixel_area
        FROM   violations
        WHERE  segment_id IN ({fmt})
          AND  label IN ('pothole', 'alligator_crack', 'longitudinal_crack')
        GROUP  BY segment_id, label
    """, seg_ids)

    # Index: segment_id → label → {cnt, pixel_area}
    agg = {}
    for row in cur.fetchall():
        agg.setdefault(row["segment_id"], {})[row["label"]] = {
            "cnt":        int(row["cnt"] or 0),
            "pixel_area": int(row["pixel_area"] or 0),
        }

    cur.close()
    conn.close()

    # Build CSV rows
    out_rows = []
    for i, seg in enumerate(segments, 1):
        sid   = seg["segment_id"]
        viols = agg.get(sid, {})
        pot   = viols.get("pothole",            {"cnt": 0, "pixel_area": 0})
        alli  = viols.get("alligator_crack",    {"cnt": 0, "pixel_area": 0})
        longi = viols.get("longitudinal_crack", {"cnt": 0, "pixel_area": 0})

        lm = seg["length_meters"]
        km = round(float(lm) / 1000, 3) if lm else 0.0
        out_rows.append({
            "id":                            i,
            "Segment_name":                  seg["segment_name"] or f"Segment#{sid[:8]}",
            "Total_frames_in_segment":       seg["frame_count"] or 0,
            "Distance_covered_km":           km,
            "Pothole_count":                 pot["cnt"],
            "Pothole_total_area_pixels (all detections combined)":           pot["pixel_area"],
            "Alligator_crack_count":         alli["cnt"],
            "Alligator_crack_total_area_pixels (all detections combined)":   alli["pixel_area"],
            "Longitudinal_crack_count":      longi["cnt"],
            "Longitudinal_crack_total_area_pixels (all detections combined)": longi["pixel_area"],
        })

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"Saved {len(out_rows)} rows → {args.out}")


if __name__ == "__main__":
    main()
