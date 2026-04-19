"""
FastAPI app for spain-map.

Endpoints (all served under /api/ via Caddy reverse proxy):
  GET /api/meta                              — layer catalogue with availability flags + counts
  GET /api/counts?bbox=w,s,e,n               — per-layer counts for viewport, all layers at once
  GET /api/points?layer=X&bbox=&zoom=&limit= — raw points or server-clustered points
  GET /api/heatmap?layer=X&bbox=             — heatmap cells for viewport
  GET /api/health                            — liveness probe

Response shape for /api/points:
  { layer, bbox, zoom, clustered: bool, points: [...] }
  - clustered=false: [{lat,lon,name,city,address,website,phone,kind}, ...]
  - clustered=true:  [{lat,lon,count}, ...]
"""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from . import config, storage

app = FastAPI(title="spain-map", version="0.1.0")


# ---------- helpers ----------

def _parse_bbox(bbox: str) -> tuple[float, float, float, float]:
    try:
        parts = [float(x) for x in bbox.split(",")]
        if len(parts) != 4:
            raise ValueError
        w, s, e, n = parts
    except ValueError:
        raise HTTPException(400, "bbox must be 'west,south,east,north' floats")
    if not (-180 <= w < e <= 180) or not (-90 <= s < n <= 90):
        raise HTTPException(400, "bbox coordinates out of range or inverted")
    if (e - w) * (n - s) > config.MAX_BBOX_AREA_DEG2:
        raise HTTPException(400, f"bbox area too large (>{config.MAX_BBOX_AREA_DEG2}°²)")
    return w, s, e, n


def _cache_headers(seconds: int = config.CACHE_SECONDS) -> dict:
    return {"Cache-Control": f"public, max-age={seconds}"}


# ---------- endpoints ----------

@app.get("/api/health")
def health() -> dict:
    return {"ok": True}


@app.get("/api/meta")
def meta() -> JSONResponse:
    """Layer catalogue: which layers exist, with totals. Called once on frontend boot."""
    point_layers = set(storage.available_point_layers())
    heatmap_layers = set(storage.available_heatmap_layers())
    layers = []
    for lid, meta in config.LAYER_CATALOGUE.items():
        if meta["type"] == "points":
            available = lid in point_layers
        else:
            available = lid in heatmap_layers
        entry = {"id": lid, **meta, "available": available}
        if available and meta["type"] == "heatmap":
            entry["meta"] = storage.heatmap_meta(lid)
        layers.append(entry)
    return JSONResponse({"layers": layers}, headers=_cache_headers(300))


@app.get("/api/counts")
def counts(bbox: str = Query(..., description="west,south,east,north")) -> JSONResponse:
    """Per-layer counts for current viewport. Two multi-file queries (points + heatmap)."""
    w, s, e, n = _parse_bbox(bbox)
    raw = storage.counts_all_in_bbox(w, s, e, n)
    # normalize: every catalogue layer gets an entry (0 if no parquet / no points in bbox)
    out = {lid: int(raw.get(lid, 0)) for lid in config.LAYER_CATALOGUE}
    return JSONResponse({"bbox": [w, s, e, n], "counts": out}, headers=_cache_headers())


@app.get("/api/points")
def points(
    layer: str,
    bbox: str = Query(..., description="west,south,east,north"),
    zoom: int = Query(..., ge=0, le=22),
    limit: int = Query(config.RAW_POINT_LIMIT, ge=1, le=config.MAX_RESPONSE_POINTS),
) -> JSONResponse:
    """
    Returns either raw points (if total in bbox ≤ limit) or server-side clusters.

    Clustering strategy: grid-aggregate by cell_deg from config.cluster_cell_deg(zoom).
    """
    meta = config.LAYER_CATALOGUE.get(layer)
    if not meta or meta["type"] != "points":
        raise HTTPException(404, f"unknown points layer '{layer}'")
    w, s, e, n = _parse_bbox(bbox)
    total = storage.count_points_in_bbox(layer, w, s, e, n)
    if total <= limit:
        pts = storage.fetch_points_in_bbox(layer, w, s, e, n, limit)
        return JSONResponse(
            {"layer": layer, "bbox": [w, s, e, n], "zoom": zoom, "clustered": False,
             "total": total, "points": pts},
            headers=_cache_headers(),
        )
    cell = config.cluster_cell_deg(zoom)
    clusters = storage.cluster_points_in_bbox(layer, w, s, e, n, cell, config.MAX_RESPONSE_POINTS)
    return JSONResponse(
        {"layer": layer, "bbox": [w, s, e, n], "zoom": zoom, "clustered": True,
         "total": total, "cell_deg": cell, "points": clusters},
        headers=_cache_headers(),
    )


@app.get("/api/heatmap")
def heatmap(layer: str) -> JSONResponse:
    """
    Heatmap layers are small (≤5k cells) and need provinces[] + legend metadata for choropleth
    rendering. We serve the original JSON untouched — simpler than reconstructing from parquet.
    """
    meta = config.LAYER_CATALOGUE.get(layer)
    if not meta or meta["type"] != "heatmap":
        raise HTTPException(404, f"unknown heatmap layer '{layer}'")
    filename = config.HEATMAP_JSON_FILES.get(layer)
    if not filename:
        raise HTTPException(404, f"no source file mapped for '{layer}'")
    path = config.DATA_DIR / filename
    if not path.exists():
        raise HTTPException(404, f"source file {filename} not found")
    import json as _json
    payload = _json.loads(path.read_text(encoding="utf-8"))
    return JSONResponse(payload, headers=_cache_headers(300))


# Optional static mount for dev: when SPAIN_MAP_STATIC is set to a directory containing
# index.html, FastAPI also serves the frontend so dev == prod (no separate Caddy needed).
# In production Caddy handles static files; this path stays unused. Must be declared LAST —
# a root-mount catches any path not already matched by an /api/* route above.
_static_dir = os.environ.get("SPAIN_MAP_STATIC")
if _static_dir and Path(_static_dir).is_dir():
    app.mount("/", StaticFiles(directory=_static_dir, html=True), name="static")
