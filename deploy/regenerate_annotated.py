#!/usr/bin/env python3
"""
Redraw the annotated images from the `violations` table.

The annotated JPEGs are derived artefacts — processing_worker draws them from
model output. But every input that draw_annotations() needs is already stored:
label, bbox_xmin/ymin/xmax/ymax and polygon_points. So they can be rebuilt from
a restored dump plus the original images, with no model server calls and no
re-inference.

Drawing logic mirrors draw_annotations() in processing_worker/tasks.py — keep
the two in sync if the palette or box style changes there.

Run inside the download_worker image (it has Pillow + mysql-connector):

    docker compose run --rm --no-deps \
      -v "$PWD/downloaded_images:/app/downloaded_images" \
      download_worker python /app/deploy/regenerate_annotated.py
"""
import json
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import mysql.connector
from PIL import Image, ImageDraw

DOWNLOAD_DIR  = os.environ.get("DOWNLOAD_DIR",  "/app/downloaded_images")
ANNOTATED_DIR = os.environ.get("ANNOTATED_DIR", "/app/downloaded_images/annotated")

MYSQL_HOST = os.environ.get("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3306))
MYSQL_DB   = os.environ.get("MYSQL_DB", "pci")
MYSQL_USER = os.environ.get("MYSQL_USER", "pci_user")
MYSQL_PASS = os.environ.get("MYSQL_PASSWORD", "pci_pass")

WORKERS = int(os.environ.get("DRAW_WORKERS", 16))
FORCE   = os.environ.get("FORCE", "") == "1"
# Smoke-test knob: LIMIT=20 redraws only 20 frames' worth of violations.
LIMIT   = int(os.environ.get("LIMIT", 0))
# Optional scoping, e.g. MUNICIPALITIES=002001,005001 for Makkah + Jeddah.
MUNICIPALITIES = [m.strip() for m in os.environ.get("MUNICIPALITIES", "").split(",") if m.strip()]

LABEL_COLORS = {
    "pothole":            (255,  50,  50),
    "longitudinal_crack": (255, 165,   0),
    "transverse_crack":   (255, 255,   0),
    "alligator_crack":    (255,   0, 255),
    "rutting":            (  0, 200, 255),
    "default":            (  0, 255,   0),
}

os.makedirs(ANNOTATED_DIR, exist_ok=True)


def get_color(label):
    return LABEL_COLORS.get((label or "").lower(), LABEL_COLORS["default"])


def draw_one(frame_id, dets):
    src = os.path.join(DOWNLOAD_DIR, f"{frame_id}.jpg")
    out = os.path.join(ANNOTATED_DIR, f"{frame_id}.jpg")

    if not os.path.exists(src):
        return "missing", frame_id
    if os.path.exists(out) and os.path.getsize(out) > 0 and not FORCE:
        return "skip", frame_id

    try:
        img  = Image.open(src).convert("RGB")
        draw = ImageDraw.Draw(img, "RGBA")

        for label, xmin, ymin, xmax, ymax, polygon in dets:
            color   = get_color(label)
            color_t = color + (70,)

            if polygon and len(polygon) >= 3:
                draw.polygon([tuple(p) for p in polygon],
                             fill=color_t, outline=color + (255,))

            if None not in (xmin, ymin, xmax, ymax):
                draw.rectangle([xmin, ymin, xmax, ymax], outline=color, width=3)
                text   = label or "unknown"
                tx, ty = xmin, max(ymin - 20, 0)
                tw     = len(text) * 7 + 8
                draw.rectangle([tx, ty, tx + tw, ty + 18], fill=color)
                draw.text((tx + 4, ty + 2), text, fill=(0, 0, 0))

        tmp = f"{out}.part"
        img.save(tmp, "JPEG", quality=90)
        os.replace(tmp, out)
        return "ok", frame_id
    except Exception as exc:
        return "fail", f"{frame_id}\t{type(exc).__name__}: {exc}"


def main():
    conn = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, database=MYSQL_DB,
        user=MYSQL_USER, password=MYSQL_PASS,
    )
    cur = conn.cursor()
    sql = """
        SELECT v.frame_id, v.label, v.bbox_xmin, v.bbox_ymin,
               v.bbox_xmax, v.bbox_ymax, v.polygon_points
        FROM violations v
    """
    params = []
    if MUNICIPALITIES:
        sql += " JOIN frames f ON f.frame_id = v.frame_id WHERE f.municipality IN (%s)" \
               % ",".join(["%s"] * len(MUNICIPALITIES))
        params = MUNICIPALITIES
    sql += " ORDER BY v.frame_id"
    cur.execute(sql, params)

    by_frame = defaultdict(list)
    for frame_id, label, x0, y0, x1, y1, poly in cur:
        pts = []
        if poly:
            try:
                pts = json.loads(poly) if isinstance(poly, (str, bytes)) else poly
            except (ValueError, TypeError):
                pts = []
        by_frame[frame_id].append((label, x0, y0, x1, y1, pts))

    cur.close()
    conn.close()

    if LIMIT:
        by_frame = dict(list(by_frame.items())[:LIMIT])

    total = len(by_frame)
    scope = ",".join(MUNICIPALITIES) if MUNICIPALITIES else "all municipalities"
    print(f"==> {total} frames have violations ({scope}), target {ANNOTATED_DIR}")
    print(f"    {WORKERS} workers, force={'yes' if FORCE else 'no'}")

    counts   = defaultdict(int)
    failures = []
    missing  = []

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(draw_one, fid, dets)
                   for fid, dets in by_frame.items()]
        for i, fut in enumerate(as_completed(futures), 1):
            kind, payload = fut.result()
            counts[kind] += 1
            if kind == "fail":
                failures.append(payload)
            elif kind == "missing":
                missing.append(payload)
            if i % 500 == 0 or i == total:
                print(f"  {i}/{total}  drawn={counts['ok']} skipped={counts['skip']} "
                      f"missing_src={counts['missing']} failed={counts['fail']}",
                      flush=True)

    print(f"\n==> drawn={counts['ok']} skipped={counts['skip']} "
          f"missing_src={counts['missing']} failed={counts['fail']}")

    if missing:
        print(f"    {len(missing)} frames had no original on disk — "
              f"run deploy/fetch_images.py first, then re-run this.")
    if failures:
        out = os.path.join(ANNOTATED_DIR, "draw_failures.tsv")
        with open(out, "w") as fh:
            fh.write("\n".join(failures) + "\n")
        print(f"    failures written to {out}")
        sys.exit(1)


if __name__ == "__main__":
    main()
