"""
streamlit_app/dashboard.py
===========================
PCI Road Defect Live Dashboard.

Auto-refreshes every N seconds (configurable in sidebar) by re-querying MySQL
and re-rendering the Folium map. No manual page refresh required.

Approach: streamlit-autorefresh injects a JS timer that triggers st.rerun()
on the configured interval. st.cache_data(ttl=10) ensures DB queries are not
hammered on every rerun — data is cached for 10s and refreshed only when stale.
"""

import os
import json
import math
from datetime import datetime
from collections import defaultdict

import streamlit as st
import pandas as pd
import pydeck as pdk
import folium
from streamlit_folium import st_folium
from streamlit_autorefresh import st_autorefresh
import mysql.connector

st.set_page_config(
    page_title="PCI Road Defect Dashboard",
    page_icon="🛣",
    layout="wide",
)

# ─── Auto-refresh (must run before any widget) ────────────────────────────────
_REFRESH_SEC = 60
st_autorefresh(interval=_REFRESH_SEC * 1_000, key="pci_map_refresh")

# Label Studio-style light theme
st.markdown("""
<style>
/* ─── Hide Streamlit chrome ──────────────────────────────────────────────── */
[data-testid="stDeployButton"]  { display: none !important; }
[data-testid="stToolbar"]       { display: none !important; }
#MainMenu                        { visibility: hidden; }
footer                           { visibility: hidden; }
iframe[title="streamlit_autorefresh.st_autorefresh"] { display: none !important; }
.element-container:has(iframe[title="streamlit_autorefresh.st_autorefresh"]) { display: none !important; }

/* ─── Base page ──────────────────────────────────────────────────────────── */
.stApp, [data-testid="stAppViewContainer"] {
    background: #fafafa !important;
}
.block-container {
    padding-top: 1.6rem !important;
    padding-bottom: 2rem !important;
    background: #fafafa !important;
}

/* ─── Header ─────────────────────────────────────────────────────────────── */
.pci-header { display: flex; align-items: center; gap: 14px; margin-bottom: 4px; }
.pci-header h1 {
    font-size: 1.75rem !important; font-weight: 700 !important;
    margin: 0 !important; color: #1f3b4d !important;
}
.pci-subtitle { color: #8c8ca1; font-size: 0.88rem; margin-top: 2px; margin-bottom: 18px; }

/* ─── KPI cards ──────────────────────────────────────────────────────────── */
.kpi-row { display: flex; gap: 12px; margin-bottom: 18px; flex-wrap: wrap; }
.kpi-card {
    flex: 1; min-width: 140px;
    background: #ffffff;
    border: 1px solid #e8e8ee;
    border-radius: 8px;
    padding: 16px 18px;
    position: relative; overflow: hidden;
    box-shadow: 0 1px 4px rgba(31,59,77,0.07);
    transition: box-shadow .15s;
}
.kpi-card:hover { box-shadow: 0 3px 10px rgba(31,59,77,0.12); }
.kpi-card::before {
    content: ""; position: absolute; top: 0; left: 0; right: 0; height: 3px;
    background: var(--accent, #617ae6);
    border-radius: 8px 8px 0 0;
}
.kpi-icon { font-size: 1.2rem; margin-bottom: 8px; opacity: 0.8; }
.kpi-value { font-size: 1.8rem; font-weight: 700; color: #1f3b4d; line-height: 1.1; }
.kpi-label { font-size: 0.72rem; color: #8c8ca1; margin-top: 5px; font-weight: 600;
    letter-spacing: .04em; text-transform: uppercase; }

/* ─── Map card ───────────────────────────────────────────────────────────── */
.map-card {
    border: 1px solid #e8e8ee; border-radius: 8px; overflow: hidden;
    margin-bottom: 20px;
    box-shadow: 0 1px 4px rgba(31,59,77,0.07);
}
iframe { border-radius: 0 !important; }

[data-testid="stExpander"] {
    border: 1px solid #e8e8ee !important; border-radius: 8px !important;
    background: #ffffff !important;
}

/* ─── Tabs — Label Studio underline style ────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    border-bottom: 2px solid #e8e8ee;
    background: transparent;
}
.stTabs [data-baseweb="tab"] {
    height: 42px; border-radius: 0; padding: 0 20px;
    background: transparent !important;
    color: #8c8ca1 !important; font-weight: 600; font-size: 0.9rem;
    border-bottom: 2px solid transparent; margin-bottom: -2px;
}
.stTabs [aria-selected="true"] {
    background: transparent !important; color: #617ae6 !important;
    border-bottom: 2px solid #617ae6 !important;
}
.stTabs [data-baseweb="tab"]:hover { color: #1f3b4d !important; }

/* ─── Buttons ────────────────────────────────────────────────────────────── */
.stButton > button {
    background: #ffffff; color: #1f3b4d;
    border: 1px solid #d9d9e3; border-radius: 6px;
    font-weight: 600; font-size: 0.85rem;
}
.stButton > button:hover {
    background: #f0f0f8; border-color: #617ae6; color: #617ae6;
}

/* ─── Sidebar ────────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: #ffffff !important;
    border-right: 1px solid #e8e8ee !important;
}
[data-testid="stSidebar"] > div { background: #ffffff !important; }
[data-testid="stSidebar"] * { color: #1f3b4d; }

.sb-brand { display:flex; align-items:center; gap:10px; margin-bottom:2px; }
.sb-brand .em { font-size:1.5rem; }
.sb-brand .tt { font-weight:700; font-size:1.1rem; color:#1f3b4d; }
.sb-sub { color:#8c8ca1; font-size:0.77rem; margin-bottom:12px; }
.sb-section {
    font-size:0.68rem; font-weight:700; letter-spacing:.07em; color:#8c8ca1;
    text-transform:uppercase; margin: 14px 0 8px;
    border-top: 1px solid #f0f0f5; padding-top: 12px;
}
.sb-section:first-of-type { border-top: none; padding-top: 0; }

.legend-row { display:flex; align-items:center; justify-content:space-between; margin:5px 0; font-size:0.84rem; color:#1f3b4d; }
.legend-left { display:flex; align-items:center; gap:8px; }
.legend-dot { width:9px; height:9px; border-radius:50%; flex-shrink:0; }
.legend-count {
    color:#8c8ca1; font-size:0.76rem;
    background:#f0f0f8; padding:1px 8px; border-radius:10px;
    font-weight:600;
}

/* ─── Severity pill ──────────────────────────────────────────────────────── */
.sev-badge { padding:2px 9px; border-radius:20px; font-size:11px; font-weight:700; letter-spacing:.02em; }

/* ─── Dataframe / table overrides ────────────────────────────────────────── */
[data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ─── Config ───────────────────────────────────────────────────────────────────
_DB_CONFIG = {
    "host":               os.environ.get("MYSQL_HOST",     "mysql"),
    "port":               int(os.environ.get("MYSQL_PORT",  3306)),
    "user":               os.environ.get("MYSQL_USER",     "pci_user"),
    "password":           os.environ.get("MYSQL_PASSWORD", "pci_pass"),
    "database":           os.environ.get("MYSQL_DB",       "pci"),
    "charset":            "utf8mb4",
    "connection_timeout": 10,
}

# Public URL used in popup links — must be reachable from the user's browser
FLASK_API_PUBLIC = os.environ.get("FLASK_API_PUBLIC", "http://localhost:5000")

CRACK_LABELS = {
    "longitudinal_crack", "transverse_crack",
    "alligator_crack", "rutting", "road_crack",
}

LABEL_COLORS = {
    "pothole":            "#f85149",
    "longitudinal_crack": "#ffa657",
    "transverse_crack":   "#ffdd57",
    "alligator_crack":    "#bf40bf",
    "rutting":            "#00c8ff",
    "road_crack":         "#ffa657",
}
_DEFAULT_COLOR = "#3fb950"

# ─── DB helpers ───────────────────────────────────────────────────────────────

def _get_conn():
    return mysql.connector.connect(**_DB_CONFIG)


@st.cache_data(ttl=30)
def load_violations():
    try:
        conn = _get_conn()
        cur  = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT id, frame_id, segment_id,
                   label, confidence, severity,
                   length_mm, breadth_mm,
                   latitude, longitude,
                   annotated_image_path,
                   created_at
            FROM violations
            ORDER BY created_at DESC
            LIMIT 50000
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        st.warning(f"Cannot load violations from DB: {e}")
        return []


@st.cache_data(ttl=30)
def load_segments():
    try:
        conn = _get_conn()
        cur  = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT s.segment_id, s.start_lat, s.start_lon,
                   s.end_lat, s.end_lon, s.gps_path,
                   s.frame_count, s.municipality, s.submunicipality,
                   s.status, s.created_at,
                   COUNT(v.id)                  AS violation_count,
                   SUM(v.severity = 'high')     AS high_count,
                   SUM(v.severity = 'medium')   AS medium_count
            FROM segments s
            LEFT JOIN violations v ON s.segment_id = v.segment_id
            GROUP BY s.segment_id
            ORDER BY violation_count DESC
            LIMIT 10000
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        for r in rows:
            if isinstance(r.get("gps_path"), str):
                try:
                    r["gps_path"] = json.loads(r["gps_path"])
                except Exception:
                    r["gps_path"] = []
        return rows
    except Exception as e:
        st.warning(f"Cannot load segments from DB: {e}")
        return []


@st.cache_data(ttl=30)
def load_summary():
    try:
        conn = _get_conn()
        cur  = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
                COUNT(*)                   AS total,
                SUM(severity = 'high')     AS high_count,
                SUM(severity = 'medium')   AS medium_count,
                SUM(severity = 'low')      AS low_count,
                COUNT(DISTINCT segment_id) AS segment_count,
                MAX(created_at)            AS last_updated
            FROM violations
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row or {}
    except Exception:
        return {}


@st.cache_data(ttl=30)
def load_pipeline_status():
    try:
        conn = _get_conn()
        cur  = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
                COUNT(*)                        AS total,
                SUM(status = 'pending')         AS pending,
                SUM(status = 'downloaded')      AS downloaded,
                SUM(status = 'processed')       AS processed
            FROM frames
        """)
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row or {}
    except Exception:
        return {}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _marker_color(label: str) -> str:
    return LABEL_COLORS.get((label or "").lower(), _DEFAULT_COLOR)


def _norm_label(label: str) -> str:
    l = (label or "").lower()
    if l == "pothole":
        return "pothole"
    return "road_crack" if l in CRACK_LABELS else l


def _safe_int(val) -> int:
    try:
        return int(val or 0)
    except (TypeError, ValueError):
        return 0


# ─── Map builder (PyDeck — WebGL, no iframe, renders in milliseconds) ─────────

def build_deck(violations: list, segments: list) -> pdk.Deck:
    lats = [v["latitude"]  for v in violations if v.get("latitude")]
    lons = [v["longitude"] for v in violations if v.get("longitude")]

    if lats:
        center_lat = sum(lats) / len(lats)
        center_lon = sum(lons) / len(lons)
        span = max(max(lats) - min(lats), max(lons) - min(lons))
        zoom = 16 if span < 0.005 else (14 if span < 0.02 else (12 if span < 0.1 else 10))
    else:
        center_lat, center_lon, zoom = 24.83, 46.89, 12

    # ── Violation scatter points ──────────────────────────────────────────────
    viol_data = []
    for v in violations:
        if not v.get("latitude") or not v.get("longitude"):
            continue
        hx = _marker_color(v.get("label", ""))
        r, g, b = int(hx[1:3], 16), int(hx[3:5], 16), int(hx[5:7], 16)
        severity = v.get("severity", "low")
        label    = (v.get("label") or "unknown").replace("_", " ").title()
        conf     = f'{(v.get("confidence") or 0) * 100:.1f}%'
        size     = ""
        if v.get("length_mm") is not None and v.get("breadth_mm") is not None:
            size = f'{v["length_mm"]:.0f}×{v["breadth_mm"]:.0f} mm'
        fid = v.get("frame_id", "")
        viol_data.append({
            "lon":      v["longitude"], "lat": v["latitude"],
            "color":    [r, g, b, 210],
            "radius":   {"high": 12, "medium": 9}.get(severity, 6),
            "label":    label,
            "severity": severity.upper(),
            "conf":     conf,
            "size":     size,
            "frame":    fid,
            "img_url":  f"{FLASK_API_PUBLIC}/api/image/{fid}" if fid else "",
        })

    scatter = pdk.Layer(
        "ScatterplotLayer",
        data=viol_data,
        get_position=["lon", "lat"],
        get_fill_color="color",
        get_radius="radius",
        radius_scale=5,
        radius_min_pixels=4,
        radius_max_pixels=20,
        pickable=True,
        stroked=True,
        line_width_min_pixels=1,
        get_line_color=[100, 100, 100, 60],
    )

    # ── Segment path lines ────────────────────────────────────────────────────
    seg_data = []
    for seg in segments:
        path = seg.get("gps_path") or []
        if len(path) < 2:
            slat, slon = seg.get("start_lat"), seg.get("start_lon")
            elat, elon = seg.get("end_lat"),   seg.get("end_lon")
            if slat and slon and elat and elon and (slat != elat or slon != elon):
                path = [[slat, slon], [elat, elon]]
            else:
                continue

        high_count   = int(seg.get("high_count")   or 0)
        medium_count = int(seg.get("medium_count")  or 0)
        viol_count   = int(seg.get("violation_count") or 0)

        if viol_count == 0:
            color = [63, 185, 80, 200]
        elif high_count > 0:
            color = [248, 81, 73, 220]
        elif medium_count > 0:
            color = [210, 153, 34, 220]
        else:
            color = [63, 185, 80, 200]

        seg_data.append({
            "path":      [[p[1], p[0]] for p in path],   # pydeck uses [lon, lat]
            "color":     color,
            "label":     f"Segment {seg['segment_id'][:8]}",
            "severity":  "",
            "conf":      f'{viol_count} violations · {seg.get("frame_count", 0)} frames',
            "size":      "",
            "frame":     "",
        })

    paths = pdk.Layer(
        "PathLayer",
        data=seg_data,
        get_path="path",
        get_color="color",
        width_min_pixels=3,
        width_scale=1,
        pickable=True,
    )

    return pdk.Deck(
        layers=[paths, scatter],
        initial_view_state=pdk.ViewState(
            latitude=center_lat, longitude=center_lon,
            zoom=zoom, pitch=0,
        ),
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        tooltip={
            "html": (
                "<div style='"
                "font-family:-apple-system,BlinkMacSystemFont,sans-serif;"
                "font-size:12px;padding:10px 12px;"
                "background:#fff;border:1px solid #e8e8ee;"
                "border-radius:8px;box-shadow:0 4px 16px rgba(0,0,0,0.13);"
                "color:#1f3b4d;min-width:160px;max-width:220px'>"
                "<img src='{img_url}' style='width:100%;border-radius:5px;margin-bottom:8px;"
                "display:block;object-fit:cover;max-height:120px'/>"
                "<b style='font-size:13px'>{label}</b>&nbsp;"
                "<span style='color:#8c8ca1;font-size:11px'>{severity}</span><br/>"
                "<span style='color:#617ae6;font-weight:600'>{conf}</span>&nbsp;"
                "<span style='color:#8c8ca1;font-size:11px'>{size}</span><br/>"
                "<a href='{img_url}' target='_blank' style='color:#617ae6;font-size:11px;"
                "text-decoration:none;font-weight:600'>🔍 View full image →</a>"
                "</div>"
            ),
        },
    )


# ─── Fetch data (sidebar legend/filters need this, so load before rendering it) ─
violations_all  = load_violations()
segments_all    = load_segments()
summary         = load_summary()
pipeline        = load_pipeline_status()

_type_counts = defaultdict(int)
for _v in violations_all:
    _type_counts[(_v.get("label") or "unknown").lower()] += 1

_sev_counts = defaultdict(int)
for _v in violations_all:
    _sev_counts[_v.get("severity", "low")] += 1

_seg_labels = {
    s["segment_id"]: (
        f"{s['segment_id'][:8]}… · {s.get('municipality') or 'Unknown'} "
        f"({s.get('violation_count', 0)} violations)"
    )
    for s in segments_all
}

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        '<div class="sb-brand"><span class="em">🛣️</span><span class="tt">PCI Dashboard</span></div>'
        '<div class="sb-sub">Real-time road defect monitoring</div>',
        unsafe_allow_html=True,
    )

    st.markdown('<div class="sb-section">Filters</div>', unsafe_allow_html=True)

    label_filter = st.selectbox(
        "Defect type",
        options=["all", "pothole", "road_crack"],
        format_func=lambda x: {
            "all":       "All Types",
            "pothole":   "Potholes",
            "road_crack":"Road Cracks",
        }.get(x, x),
    )

    sev_filter = st.multiselect(
        "Severity",
        options=["high", "medium", "low"],
        default=["high", "medium", "low"],
        format_func=lambda x: {"high": "🔴 High", "medium": "🟠 Medium", "low": "🟢 Low"}.get(x, x),
    )

    segment_filter = st.multiselect(
        "Road segment",
        options=list(_seg_labels.keys()),
        default=[],
        format_func=lambda sid: _seg_labels.get(sid, sid),
        placeholder="All segments",
    )

    show_segs = st.checkbox("Show road segments", value=True)

    st.markdown('<div class="sb-section">Actions</div>', unsafe_allow_html=True)
    if st.button("🗑️ Clear Cache & Reload", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown('<div class="sb-section">Defect Types</div>', unsafe_allow_html=True)
    for lbl, clr in LABEL_COLORS.items():
        cnt = _type_counts.get(lbl, 0)
        if cnt == 0 and lbl not in ("pothole",):
            continue
        st.markdown(
            f'<div class="legend-row"><div class="legend-left">'
            f'<div class="legend-dot" style="background:{clr}"></div>'
            f'<span>{lbl.replace("_"," ").title()}</span></div>'
            f'<span class="legend-count">{cnt}</span></div>',
            unsafe_allow_html=True,
        )
    for seg_clr, seg_lbl in (
        ("#3fb950", "Segment — no / low violations"),
        ("#d29922", "Segment — medium violations"),
        ("#f85149", "Segment — high violations"),
    ):
        st.markdown(
            f'<div class="legend-row"><div class="legend-left">'
            f'<div style="width:14px;height:3px;background:{seg_clr};border-radius:2px"></div>'
            f'<span style="font-size:0.82rem">{seg_lbl}</span></div></div>',
            unsafe_allow_html=True,
        )

    st.markdown('<div class="sb-section">Severity Mix</div>', unsafe_allow_html=True)
    _sev_total = sum(_sev_counts.values()) or 1
    for sev, clr in (("high", "#f85149"), ("medium", "#d29922"), ("low", "#3fb950")):
        pct = 100 * _sev_counts.get(sev, 0) / _sev_total
        st.markdown(
            f'<div style="margin:6px 0">'
            f'<div style="display:flex;justify-content:space-between;font-size:0.8rem;margin-bottom:3px">'
            f'<span style="color:#1f3b4d">{sev.title()}</span>'
            f'<span style="color:#8c8ca1;font-weight:600">{_sev_counts.get(sev,0)}</span></div>'
            f'<div style="background:#f0f0f8;border-radius:6px;height:5px;overflow:hidden">'
            f'<div style="width:{pct:.0f}%;height:100%;background:{clr};border-radius:6px"></div>'
            f'</div></div>',
            unsafe_allow_html=True,
        )

# ─── Apply filters ──────────────────────────────────────────────────────────────
violations = [
    v for v in violations_all
    if (label_filter == "all" or _norm_label(v.get("label")) == label_filter)
    and (not sev_filter or v.get("severity", "low") in sev_filter)
    and (not segment_filter or v.get("segment_id") in segment_filter)
]

segments_shown = [
    s for s in segments_all
    if not segment_filter or s.get("segment_id") in segment_filter
] if show_segs else []

# ─── Header ───────────────────────────────────────────────────────────────────
_h_col, _btn_col = st.columns([8, 1])
with _h_col:
    st.markdown(
        '<div class="pci-header"><h1>🛣️ PCI Road Defect Live Map</h1></div>'
        f'<div class="pci-subtitle">Auto-refreshing every {_REFRESH_SEC}s &nbsp;·&nbsp; '
        f'{len(violations)} of {len(violations_all)} violations shown</div>',
        unsafe_allow_html=True,
    )
with _btn_col:
    st.markdown("<div style='padding-top:10px'>", unsafe_allow_html=True)
    if st.button("🗑️ Clear Cache", use_container_width=True, help="Wipe cached data and reload fresh from DB"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

# ─── KPI metrics ──────────────────────────────────────────────────────────────
last_ts = str(summary.get("last_updated") or "")

kpis = [
    ("📊", "Total Violations", _safe_int(summary.get("total")),        "#617ae6"),
    ("🔴", "High Severity",    _safe_int(summary.get("high_count")),   "#f45b4f"),
    ("🟠", "Medium Severity",  _safe_int(summary.get("medium_count")), "#ff9140"),
    ("🟢", "Low Severity",     _safe_int(summary.get("low_count")),    "#1ac7ac"),
    ("🛤️", "Road Segments",    _safe_int(summary.get("segment_count")),"#617ae6"),
    ("🕒", "Last Processed",   last_ts[:16] if last_ts else "—",       "#8c8ca1"),
]

pipeline_kpis = [
    ("📥", "Total Frames",  _safe_int(pipeline.get("total")),      "#617ae6"),
    ("⏳", "Pending",       _safe_int(pipeline.get("pending")),    "#ff9140"),
    ("⬇️", "Downloaded",   _safe_int(pipeline.get("downloaded")), "#8c8ca1"),
    ("✅", "Processed",     _safe_int(pipeline.get("processed")),  "#1ac7ac"),
]

cards_html = '<div class="kpi-row">'
for icon, label, value, accent in kpis:
    cards_html += (
        f'<div class="kpi-card" style="--accent:{accent}">'
        f'<div class="kpi-icon">{icon}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="kpi-label">{label}</div>'
        f'</div>'
    )
cards_html += '</div>'
st.markdown(cards_html, unsafe_allow_html=True)

# Pipeline status row
pipe_html = '<div class="kpi-row">'
for icon, label, value, accent in pipeline_kpis:
    pipe_html += (
        f'<div class="kpi-card" style="--accent:{accent}">'
        f'<div class="kpi-icon">{icon}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="kpi-label">{label}</div>'
        f'</div>'
    )
pipe_html += '</div>'
st.markdown(pipe_html, unsafe_allow_html=True)

SEV_EMOJI = {"high": "🔴 High", "medium": "🟠 Medium", "low": "🟢 Low"}

# ─── Tabs ───────────────────────────────────────────────────────────────────────
if not violations_all and not segments_all:
    st.info(
        "No data in the database yet. "
        "Upload frames via the `/upload` API and processed violations will appear here automatically."
    )
else:
    tab_map, tab_violations, tab_segments = st.tabs([
        "🗺️  Map View",
        f"📋  Violations ({len(violations)})",
        f"🛤️  Segments ({len(segments_all)})",
    ])

    with tab_map:
        MAP_LIMIT = 5000
        v_for_map = violations[:MAP_LIMIT]
        deck = build_deck(v_for_map, segments_shown)
        st.markdown('<div class="map-card">', unsafe_allow_html=True)
        st.pydeck_chart(deck, height=620)
        st.markdown('</div>', unsafe_allow_html=True)
        cap = "🟢 No violations · 🟠 Medium · 🔴 High severity segment · Hover a point to inspect"
        if len(violations) > MAP_LIMIT:
            cap += f" · Showing {MAP_LIMIT} of {len(violations)} violations on map"
        st.caption(cap)

    with tab_violations:
        if violations:
            df = pd.DataFrame([{
                "Image":       f"{FLASK_API_PUBLIC}/api/image/{v['frame_id']}",
                "View":        f"{FLASK_API_PUBLIC}/api/image/{v['frame_id']}",
                "Type":        (v.get("label") or "").replace("_", " ").title(),
                "Severity":    SEV_EMOJI.get(v.get("severity", "low"), v.get("severity", "")),
                "Confidence":  (v.get("confidence") or 0),
                "Length (mm)": v.get("length_mm"),
                "Breadth (mm)":v.get("breadth_mm"),
                "Lat":         v.get("latitude", 0),
                "Lon":         v.get("longitude", 0),
                "Detected":    str(v.get("created_at") or "")[:19],
                "Frame ID":    v["frame_id"],
            } for v in violations])
            st.dataframe(
                df, use_container_width=True, hide_index=True,
                column_config={
                    "Image": st.column_config.ImageColumn(
                        "Image", help="Annotated defect image",
                        width="small",
                    ),
                    "View": st.column_config.LinkColumn(
                        "View Full", display_text="🔍 Open",
                        help="Open annotated image in new tab",
                    ),
                    "Confidence": st.column_config.ProgressColumn(
                        "Confidence", format="%.0f%%", min_value=0, max_value=1,
                    ),
                    "Lat": st.column_config.NumberColumn("Lat", format="%.5f"),
                    "Lon": st.column_config.NumberColumn("Lon", format="%.5f"),
                },
            )
            st.download_button(
                "⬇ Download CSV", df.to_csv(index=False).encode(),
                file_name="pci_violations.csv", mime="text/csv",
            )
        else:
            st.info("No violations match the current filters.")

    with tab_segments:
        _segs_for_table = [
            s for s in segments_all
            if not segment_filter or s.get("segment_id") in segment_filter
        ]
        if _segs_for_table:
            df_seg = pd.DataFrame([{
                "Segment ID":   s["segment_id"][:12] + "…",
                "Municipality": s.get("municipality", ""),
                "Sub-muni":     s.get("submunicipality", ""),
                "Frames":       s.get("frame_count", 0),
                "Violations":   s.get("violation_count", 0),
                "Status":       s.get("status", ""),
                "Created":      str(s.get("created_at") or "")[:19],
            } for s in _segs_for_table])
            st.dataframe(df_seg, use_container_width=True, hide_index=True)
        else:
            st.info("No segments match the current filters.")
