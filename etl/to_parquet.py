# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb>=1.1.0"]
# ///
"""
Convert legacy data/*.json files into Parquet partitions consumed by server/.

Reads     data/*.json            (written by scripts/fetch_*.py)
Writes    data/parquet/points/<layer>.parquet
          data/parquet/heatmap/<layer>.parquet
          data/parquet/heatmap/<layer>.meta.json   (legend / render hints)

Idempotent. Safe to re-run. Skips layers whose source file is missing or empty.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import duckdb

DATA = Path(__file__).resolve().parent.parent / "data"
OUT_POINTS = DATA / "parquet" / "points"
OUT_HEAT = DATA / "parquet" / "heatmap"

POINT_LAYERS: dict[str, str] = {
    "schools": "osm_schools.json",
    "neae": "schools_neae.json",
    "hospitals": "osm_hospitals.json",
    "spanish": "osm_spanish.json",
    "yoga": "osm_yoga.json",
    "slp": "osm_slp.json",
    "hippo": "hippo.json",
    "enduro": "enduro.json",
    "neurorehab": "neurorehab.json",
    "speech": "speech_ru.json",
}

HEATMAP_LAYERS: dict[str, str] = {
    "internet_speed": "internet_speed.json",
    "rent_cost": "rent_cost.json",
    "hate_crimes": "hate_crimes.json",
    "power_outages": "power_outages.json",
}

# keys promoted to their own columns on point layers; everything else goes into props_json
POINT_COLUMNS = ("name", "city", "address", "website", "phone", "kind")


def _lon(feat: dict) -> float | None:
    v = feat.get("lng", feat.get("lon"))
    return float(v) if v is not None else None


def _lat(feat: dict) -> float | None:
    v = feat.get("lat")
    return float(v) if v is not None else None


def convert_points(layer_id: str, src: Path, con: duckdb.DuckDBPyConnection) -> None:
    if not src.exists():
        print(f"[skip] points/{layer_id}: {src.name} not found")
        return
    doc = json.loads(src.read_text(encoding="utf-8"))
    feats = doc.get("features", []) if isinstance(doc, dict) else []
    rows = []
    for f in feats:
        lat, lon = _lat(f), _lon(f)
        if lat is None or lon is None:
            continue
        promoted = {k: f.get(k) for k in POINT_COLUMNS}
        extras = {k: v for k, v in f.items() if k not in (*POINT_COLUMNS, "lat", "lng", "lon")}
        rows.append({
            "lat": lat,
            "lon": lon,
            **{k: (str(v) if v is not None else None) for k, v in promoted.items()},
            "props_json": json.dumps(extras, ensure_ascii=False) if extras else None,
        })
    if not rows:
        print(f"[skip] points/{layer_id}: 0 rows")
        return
    OUT_POINTS.mkdir(parents=True, exist_ok=True)
    out = OUT_POINTS / f"{layer_id}.parquet"
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tf:
        json.dump(rows, tf, ensure_ascii=False)
        tmp_path = tf.name
    try:
        con.execute(
            f"""
            COPY (
                SELECT * FROM read_json_auto('{tmp_path}', maximum_object_size=209715200)
                ORDER BY lat, lon
            )
            TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    finally:
        Path(tmp_path).unlink(missing_ok=True)
    print(f"[ok]   points/{layer_id}: {len(rows):>6} rows → {out.relative_to(DATA.parent)}")


def convert_heatmap(layer_id: str, src: Path, con: duckdb.DuckDBPyConnection) -> None:
    if not src.exists():
        print(f"[skip] heatmap/{layer_id}: {src.name} not found")
        return
    doc = json.loads(src.read_text(encoding="utf-8"))
    points = doc.get("points", []) if isinstance(doc, dict) else []
    rows = []
    for p in points:
        if len(p) < 3:
            continue
        rows.append({
            "lat": float(p[0]),
            "lon": float(p[1]),
            "weight": float(p[2]),
            "value": float(p[3]) if len(p) > 3 and p[3] is not None else None,
        })
    if not rows:
        print(f"[skip] heatmap/{layer_id}: 0 rows")
        return
    OUT_HEAT.mkdir(parents=True, exist_ok=True)
    out = OUT_HEAT / f"{layer_id}.parquet"
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tf:
        json.dump(rows, tf, ensure_ascii=False)
        tmp_path = tf.name
    try:
        con.execute(
            f"""
            COPY (
                SELECT * FROM read_json_auto('{tmp_path}')
                ORDER BY lat, lon
            )
            TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    meta_keys = (
        "source", "source_url", "license", "period", "metric", "granularity",
        "legend_min", "legend_max", "legend_sub", "render_radius", "render_blur",
    )
    meta = {k: doc.get(k) for k in meta_keys if k in doc}
    meta["count"] = len(rows)
    (OUT_HEAT / f"{layer_id}.meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[ok]   heatmap/{layer_id}: {len(rows):>6} rows → {out.relative_to(DATA.parent)}")


def main() -> int:
    con = duckdb.connect()
    for lid, fn in POINT_LAYERS.items():
        convert_points(lid, DATA / fn, con)
    for lid, fn in HEATMAP_LAYERS.items():
        convert_heatmap(lid, DATA / fn, con)
    return 0


if __name__ == "__main__":
    sys.exit(main())
