"""Runtime configuration — paths, clustering thresholds, layer catalogue."""
from __future__ import annotations

import os
from pathlib import Path

# DATA_DIR must contain parquet/points/, parquet/heatmap/, provinces_es.geojson
DATA_DIR = Path(os.environ.get("SPAIN_MAP_DATA", Path(__file__).resolve().parent.parent / "data"))
PARQUET_POINTS = DATA_DIR / "parquet" / "points"
PARQUET_HEATMAP = DATA_DIR / "parquet" / "heatmap"

# Max points returned as raw markers. Beyond that → server-side clustering.
RAW_POINT_LIMIT = 2000

# Hard ceiling on any single response (even clusters).
MAX_RESPONSE_POINTS = 5000

# Cache-Control seconds for /counts and /points. Data refreshes infrequently.
CACHE_SECONDS = 60

# Viewport-area cap (deg²). Requests spanning more are rejected — the world isn't relevant.
MAX_BBOX_AREA_DEG2 = 20.0 * 20.0  # ~covers Iberia + margin


# Cluster cell size (degrees) per Leaflet zoom level. Tuned so ~64 clusters fit a viewport.
def cluster_cell_deg(zoom: int) -> float:
    # Map zoom 4..14 → cell size in degrees.
    # At zoom 7 (Spain overview) cells are ~0.8°, so a layer of 30k schools collapses to ~40 clusters.
    # At zoom 12 cells are ~0.025°, nearly point-level.
    return max(0.008, 40.0 / (2 ** zoom))


# Layer metadata — exposed via /api/meta. zoom_min: below this, we just return cluster counts (no raw).
LAYER_CATALOGUE: dict[str, dict] = {
    # points
    "schools":    {"type": "points", "label": "Школы для детей",                 "icon": "🎒", "color": "#e74c3c", "zoom_min": 0},
    "neae":       {"type": "points", "label": "Школы с поддержкой NEAE",         "icon": "🧩", "color": "#2980b9", "zoom_min": 0},
    "hospitals":  {"type": "points", "label": "Больницы",                        "icon": "🏥", "color": "#c2185b", "zoom_min": 0},
    "slp":        {"type": "points", "label": "Логопеды / речевая терапия",      "icon": "🗣️", "color": "#1abc9c", "zoom_min": 0},
    "spanish":    {"type": "points", "label": "Центры испанского языка",         "icon": "📚", "color": "#27ae60", "zoom_min": 0},
    "yoga":       {"type": "points", "label": "Йога-студии",                     "icon": "🧘", "color": "#16a085", "zoom_min": 0},
    "hippo":      {"type": "points", "label": "Лечение лошадьми",                "icon": "🐴", "color": "#8e6a3f", "zoom_min": 0},
    "enduro":     {"type": "points", "label": "Вело-эндуро (MTB)",               "icon": "🚵", "color": "#7f8c8d", "zoom_min": 0},
    "neurorehab": {"type": "points", "label": "Нейрореабилитационные центры",    "icon": "🧠", "color": "#9b59b6", "zoom_min": 0},
    "speech":     {"type": "points", "label": "Логопеды: CAS / диспраксия",      "icon": "💬", "color": "#f39c12", "zoom_min": 0},
    # heatmaps
    "internet_speed": {"type": "heatmap", "label": "Скорость интернета"},
    "rent_cost":      {"type": "heatmap", "label": "Стоимость аренды"},
    "hate_crimes":    {"type": "heatmap", "label": "Преступления по ненависти"},
    "power_outages":  {"type": "heatmap", "label": "Блэкаут 2025-04-28"},
}
