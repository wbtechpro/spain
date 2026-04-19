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

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

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
    """Per-layer counts for current viewport. Cheap: single query per layer, COUNT only."""
    w, s, e, n = _parse_bbox(bbox)
    out: dict[str, int] = {}
    for lid, meta in config.LAYER_CATALOGUE.items():
        if meta["type"] == "points":
            out[lid] = storage.count_points_in_bbox(lid, w, s, e, n)
        else:
            out[lid] = storage.count_heatmap_in_bbox(lid, w, s, e, n)
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
def heatmap(
    layer: str,
    bbox: str = Query(..., description="west,south,east,north"),
    limit: int = Query(config.MAX_RESPONSE_POINTS, ge=1, le=config.MAX_RESPONSE_POINTS),
) -> JSONResponse:
    """Returns [lat, lon, weight, value] cells within viewport plus layer metadata."""
    meta = config.LAYER_CATALOGUE.get(layer)
    if not meta or meta["type"] != "heatmap":
        raise HTTPException(404, f"unknown heatmap layer '{layer}'")
    w, s, e, n = _parse_bbox(bbox)
    cells = storage.fetch_heatmap_in_bbox(layer, w, s, e, n, limit)
    total = storage.count_heatmap_in_bbox(layer, w, s, e, n)
    return JSONResponse(
        {
            "layer": layer,
            "bbox": [w, s, e, n],
            "total": total,
            "points": cells,
            "meta": storage.heatmap_meta(layer),
        },
        headers=_cache_headers(),
    )
