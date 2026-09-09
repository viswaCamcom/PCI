#!/usr/bin/env python3
"""
fetch_mbs_road_geometry.py
============================
Fetches the real Makkah<->Jeddah highway centerline from OpenStreetMap
(relation 11366015, "Makkah Al Mukarramah Road" / Highway 40 / ref=40,
Ministry of Transport) via a bbox-scoped Overpass query, and saves it as a
static GeoJSON file for the map to draw as a reference overlay.

Display-only, by construction: this script has no write path to mbs_segments,
segments, or frames — it only writes app/static/mbs_road_geometry.geojson
(plus an optional audit row in mbs_road_geometry). It must never influence
frozen segment boundaries or scoring.

The full OSM relation spans the whole 1,395km national highway (Jeddah to
Dammam via Makkah/Taif/Riyadh) and times out on a full fetch — this script
scopes the query to a bounding box around just the Jeddah-Makkah stretch.

Usage:
    python3 fetch_mbs_road_geometry.py                # fetch + write file + DB audit row
    python3 fetch_mbs_road_geometry.py --dry-run       # fetch + report, write nothing
    python3 fetch_mbs_road_geometry.py --no-db         # write file only, skip DB audit row

Environment variables (same as the app containers):
    MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DB
"""

import argparse
import json
import logging
import os

import mysql.connector
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
logger = logging.getLogger(__name__)

OSM_RELATION_ID = 11366015
# Tried in order — overpass-api.de rejects requests with no User-Agent (406);
# the kumi.systems mirror worked in initial testing but can rate-limit (429).
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
OVERPASS_HEADERS = {"User-Agent": "PCI-Road-Monitor/1.0 (internal, MBS corridor overlay)"}
BBOX_BUFFER_KM = 2.0
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "app", "static", "mbs_road_geometry.geojson")


def get_conn():
    return mysql.connector.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", 3306)),
        user=os.environ.get("MYSQL_USER", "pci_user"),
        password=os.environ.get("MYSQL_PASSWORD", "pci_pass"),
        database=os.environ.get("MYSQL_DB", "pci"),
        autocommit=False,
    )


def compute_bbox(cur):
    """Prefer the frozen mbs_segments extent; fall back to live segments
    filtered by Makkah/Jeddah municipality if mbs_segments doesn't exist yet."""
    try:
        cur.execute(
            "SELECT MIN(LEAST(start_lat,end_lat)) mnla, MAX(GREATEST(start_lat,end_lat)) mxla,"
            "       MIN(LEAST(start_lon,end_lon)) mnlo, MAX(GREATEST(start_lon,end_lon)) mxlo,"
            "       COUNT(*) n FROM mbs_segments"
        )
        row = cur.fetchone()
        if row and row["n"]:
            source = "mbs_segments"
        else:
            row = None
    except mysql.connector.Error:
        row = None

    if row is None:
        cur.execute(
            "SELECT MIN(LEAST(start_lat,end_lat)) mnla, MAX(GREATEST(start_lat,end_lat)) mxla,"
            "       MIN(LEAST(start_lon,end_lon)) mnlo, MAX(GREATEST(start_lon,end_lon)) mxlo,"
            "       COUNT(*) n FROM segments WHERE municipality IN ('002001','005001')"
        )
        row = cur.fetchone()
        source = "live segments (002001/005001)"
        if not row or not row["n"]:
            raise SystemExit("No corridor segments found in either mbs_segments or live segments — nothing to scope the query to.")

    buf = BBOX_BUFFER_KM / 111.32
    bbox = (row["mnla"] - buf, row["mnlo"] - buf, row["mxla"] + buf, row["mxlo"] + buf)  # (south, west, north, east)
    logger.info("Bbox derived from %s: south=%.5f west=%.5f north=%.5f east=%.5f", source, *bbox)
    return bbox


def fetch_overpass(bbox):
    south, west, north, east = bbox
    query = (
        f'[out:json][timeout:60];'
        f'way["highway"~"motorway|trunk"]["ref"="40"]({south},{west},{north},{east});'
        f'out geom;'
    )
    logger.info("Querying Overpass (bbox-scoped, ref=40)...")
    last_exc = None
    for url in OVERPASS_URLS:
        try:
            resp = requests.get(url, params={"data": query}, headers=OVERPASS_HEADERS, timeout=90)
            resp.raise_for_status()
            data = resp.json()
            elements = data.get("elements", [])
            logger.info("Overpass (%s) returned %d way(s).", url, len(elements))
            return elements
        except Exception as exc:
            logger.warning("Overpass endpoint %s failed: %s", url, exc)
            last_exc = exc
    raise SystemExit(f"All Overpass endpoints failed. Last error: {last_exc}")


def to_geojson(elements):
    features = []
    for el in elements:
        geom = el.get("geometry") or []
        if len(geom) < 2:
            continue
        coords = [[pt["lon"], pt["lat"]] for pt in geom]
        features.append({
            "type": "Feature",
            "properties": {
                "osm_way_id": el.get("id"),
                "name": el.get("tags", {}).get("name"),
                "ref": el.get("tags", {}).get("ref"),
            },
            "geometry": {"type": "LineString", "coordinates": coords},
        })
    return {"type": "FeatureCollection", "features": features}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Fetch + report, write nothing")
    ap.add_argument("--no-db", action="store_true", help="Write file only, skip DB audit row")
    args = ap.parse_args()

    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    bbox = compute_bbox(cur)
    elements = fetch_overpass(bbox)
    if not elements:
        raise SystemExit(
            "Overpass returned 0 ways for this bbox/ref — the query filter or bbox may need adjusting. "
            "Nothing written."
        )
    geojson = to_geojson(elements)
    total_points = sum(len(f["geometry"]["coordinates"]) for f in geojson["features"])
    logger.info("Built GeoJSON: %d LineString features, %d total points.", len(geojson["features"]), total_points)

    if args.dry_run:
        logger.info("Dry-run — not writing file or DB row.")
        cur.close(); conn.close()
        return

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(geojson, f)
    logger.info("Wrote %s", OUTPUT_PATH)

    if not args.no_db:
        cur.execute(
            "INSERT INTO mbs_road_geometry (source, osm_relation_id, geojson) VALUES (%s,%s,%s)",
            ("overpass", OSM_RELATION_ID, json.dumps(geojson)),
        )
        conn.commit()
        logger.info("Recorded audit row in mbs_road_geometry.")

    cur.close(); conn.close()


if __name__ == "__main__":
    main()
