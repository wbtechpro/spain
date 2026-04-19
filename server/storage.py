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
    return sorted(p.stem for p in config.PARQUET_HEATMAP.glob("*.parquet"))


def points_parquet(layer: str) -> Path | None:
    p = config.PARQUET_POINTS / f"{layer}.parquet"
    return p if p.exists() else None


def heatmap_parquet(layer: str) -> Path | None:
    p = config.PARQUET_HEATMAP / f"{layer}.parquet"
    return p if p.exists() else None


def heatmap_meta(layer: str) -> dict:
    p = config.PARQUET_HEATMAP / f"{layer}.meta.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


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
    """Return raw point rows for a bbox, capped at `limit`."""
    path = points_parquet(layer)
    if path is None:
        return []
    rows = _cursor().execute(
        """
        SELECT lat, lon, name, city, address, website, phone, kind
        FROM read_parquet(?)
        WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?
        LIMIT ?
        """,
        [str(path), s, n, w, e, limit],
    ).fetchall()
    return [
        {
            "lat": r[0],
            "lon": r[1],
            "name": r[2],
            "city": r[3],
            "address": r[4],
            "website": r[5],
            "phone": r[6],
            "kind": r[7],
        }
        for r in rows
    ]


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
    return [{"lat": r[0], "lon": r[1], "count": int(r[2])} for r in rows]


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
