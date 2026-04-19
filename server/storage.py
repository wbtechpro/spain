"""DuckDB layer — parquet discovery, bbox queries, cluster aggregation."""
from __future__ import annotations

import json
import threading
from pathlib import Path

import duckdb

from . import config

_lock = threading.Lock()
_con: duckdb.DuckDBPyConnection | None = None


def _connect() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        with _lock:
            if _con is None:
                c = duckdb.connect(database=":memory:", read_only=False)
                c.execute("PRAGMA threads=4")
                _con = c
    return _con


def _cursor() -> duckdb.DuckDBPyConnection:
    """Per-call cursor so concurrent requests don't trample each other."""
    return _connect().cursor()


def available_point_layers() -> list[str]:
    return sorted(p.stem for p in config.PARQUET_POINTS.glob("*.parquet"))


def available_heatmap_layers() -> list[str]:
    """
    Heatmap layers are served from their source JSON (see /api/heatmap), so
    availability is determined by the JSON file existing — parquet is only
    maintained for point layers.
    """
    return sorted(
        lid for lid, fn in config.HEATMAP_JSON_FILES.items()
        if (config.DATA_DIR / fn).exists()
    )


def points_parquet(layer: str) -> Path | None:
    p = config.PARQUET_POINTS / f"{layer}.parquet"
    return p if p.exists() else None


def heatmap_parquet(layer: str) -> Path | None:
    p = config.PARQUET_HEATMAP / f"{layer}.parquet"
    return p if p.exists() else None


_META_KEYS = (
    "source", "source_url", "license", "period", "metric", "granularity",
    "legend_min", "legend_max", "legend_sub", "render_radius", "render_blur",
)


def heatmap_meta(layer: str) -> dict:
    # Prefer ETL-derived parquet sidecar; fall back to the source JSON so layers
    # that skip the ETL (muni-level choropleths) still surface metadata.
    sidecar = config.PARQUET_HEATMAP / f"{layer}.meta.json"
    if sidecar.exists():
        return json.loads(sidecar.read_text(encoding="utf-8"))
    fn = config.HEATMAP_JSON_FILES.get(layer)
    if fn:
        path = config.DATA_DIR / fn
        if path.exists():
            doc = json.loads(path.read_text(encoding="utf-8"))
            return {k: doc[k] for k in _META_KEYS if k in doc}
    return {}


def count_points_in_bbox(layer: str, w: float, s: float, e: float, n: float) -> int:
    path = points_parquet(layer)
    if path is None:
        return 0
    row = _cursor().execute(
        "SELECT count(*) FROM read_parquet(?) WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?",
        [str(path), s, n, w, e],
    ).fetchone()
    return int(row[0]) if row else 0


def fetch_points_in_bbox(
    layer: str, w: float, s: float, e: float, n: float, limit: int
) -> list[dict]:
    """Return raw point rows for a bbox. `lng` key (not `lon`) + merged props_json."""
    path = points_parquet(layer)
    if path is None:
        return []
    rows = _cursor().execute(
        """
        SELECT lat, lon, name, city, address, website, phone, kind, props_json
        FROM read_parquet(?)
        WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
        LIMIT ?
        """,
        [str(path), s, n, w, e, limit],
    ).fetchall()
    out: list[dict] = []
    for lat, lon, name, city, address, website, phone, kind, props_json in rows:
        d = {
            "lat": lat,
            "lng": lon,
            "name": name,
            "city": city,
            "address": address,
            "website": website,
            "phone": phone,
            "kind": kind,
        }
        if props_json:
            try:
                extras = json.loads(props_json)
                for k, v in extras.items():
                    if k not in d or d[k] is None:
                        d[k] = v
            except (json.JSONDecodeError, TypeError):
                pass
        out.append(d)
    return out


def cluster_points_in_bbox(
    layer: str, w: float, s: float, e: float, n: float, cell_deg: float, limit: int
) -> list[dict]:
    """Grid-aggregate points in bbox. Returns [{lat, lon, count}, ...] at cluster centroids."""
    path = points_parquet(layer)
    if path is None:
        return []
    rows = _cursor().execute(
        """
        SELECT avg(lat) AS lat, avg(lon) AS lon, count(*) AS n
        FROM read_parquet(?)
        WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
        GROUP BY floor(lat / ?)::INT, floor(lon / ?)::INT
        ORDER BY n DESC
        LIMIT ?
        """,
        [str(path), s, n, w, e, cell_deg, cell_deg, limit],
    ).fetchall()
    return [{"lat": r[0], "lng": r[1], "count": int(r[2])} for r in rows]


def fetch_heatmap_in_bbox(
    layer: str, w: float, s: float, e: float, n: float, limit: int
) -> list[list[float]]:
    """Return heatmap cells [lat, lon, weight, value] within bbox."""
    path = heatmap_parquet(layer)
    if path is None:
        return []
    rows = _cursor().execute(
        """
        SELECT lat, lon, weight, value
        FROM read_parquet(?)
        WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
        LIMIT ?
        """,
        [str(path), s, n, w, e, limit],
    ).fetchall()
    return [[r[0], r[1], r[2], r[3]] for r in rows]


def count_heatmap_in_bbox(layer: str, w: float, s: float, e: float, n: float) -> int:
    path = heatmap_parquet(layer)
    if path is None:
        return 0
    row = _cursor().execute(
        "SELECT count(*) FROM read_parquet(?) WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?",
        [str(path), s, n, w, e],
    ).fetchone()
    return int(row[0]) if row else 0


def counts_all_in_bbox(w: float, s: float, e: float, n: float) -> dict[str, int]:
    """
    One-shot: scan every parquet under parquet/{points,heatmap} with a single query each,
    grouping by filename. Much faster than 14 sequential COUNTs.
    """
    out: dict[str, int] = {}
    for folder in (config.PARQUET_POINTS, config.PARQUET_HEATMAP):
        if not folder.exists():
            continue
        glob = str(folder / "*.parquet")
        try:
            rows = _cursor().execute(
                """
                SELECT
                    regexp_extract(filename, '([^/]+)\\.parquet$', 1) AS lid,
                    count(*) AS n
                FROM read_parquet(?, filename = true)
                WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
                GROUP BY lid
                """,
                [glob, s, n, w, e],
            ).fetchall()
        except duckdb.Error:
            # folder empty or no matching files — fall through with zeros
            continue
        for lid, cnt in rows:
            out[lid] = int(cnt)
    return out
