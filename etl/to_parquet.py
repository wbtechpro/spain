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
import re
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
    "hippo": "hippo.json",
    "enduro": "enduro.json",
    "neurorehab": "neurorehab.json",
    "speech": "speech_ru.json",
    "psych": "psych.json",
    "neuro": "neuro.json",
}

HEATMAP_LAYERS: dict[str, str] = {
    "internet_speed": "internet_speed.json",
    "rent_cost": "rent_cost.json",
    "hate_crimes": "hate_crimes.json",
    "power_outages": "power_outages.json",
    "cita_previa": "cita_previa.json",
    "yoga_price": "yoga_price.json",
}

# keys promoted to their own columns on point layers; everything else goes into props_json
POINT_COLUMNS = ("name", "city", "address", "website", "phone", "kind")


# ---------- spanish-language layer: classify & filter ----------
# Источник `osm_spanish.json` — это всё `amenity=language_school` по Испании.
# Сырьё на 76% — академии английского и прочих ин.языков, не релевантные для
# релоканта, ищущего испанский. Здесь оставляем только то, что учит испанскому
# (или соофициальным), и проставляем `kind` — короткий код для чипа на карточке.
_SP_EOI       = re.compile(r"escuela oficial de idiomas|escola oficial de idiomas|hizkuntza eskola ofiziala|\beoi\b")
_SP_EUSK      = re.compile(r"euskaltegi|euskaltegui")
_SP_CATALAN   = re.compile(r"consorci.*ling|\bcpnl\b|\bcnl\b|catal[aà]")
_SP_GALICIAN  = re.compile(r"galego|galician")
_SP_DROP      = re.compile(
    r"english|ingl[eé]s|british|\bamerican\b|cambridge|oxford|wall street|"
    r"kumon|berlitz|\bwsi\b|international house|kings.*school|trinity college|"
    r"helen doron|hellen doron|kids\s*&?\s*us|kids\.us|teens\s*&?\s*us|"
    r"alliance fran|fran[cç]ais|french|goethe|deutsch|alem[aá]n|"
    r"italiano|italian|chinese|chino|mandarin|confucius|algorithmics|"
    r"direct language|one to one|talk talk|bristol|hamilton idiomas|"
    r"royal school|royal college|queen.?s|robinson|number 16|"
    r"your world|your english|english wonderland|english connection|"
    r"englisch tutor|anglia|oakland|william.?s school|williams? academy|"
    r"\bboston\b|educa[cs]hild|masterclass|aprending|maxus formaci|"
    r"novalinguae|second language acquisition|feedback institute|"
    r"a casa das linguas|escola de l[ií]nguas anglophil|"
    r"escuela de ingl[eé]s|escola d.?angl[eé]s|the unique english|"
    r"centro de apoio educativo|interlanguage|abc school"
)
_SP_CERVANTES = re.compile(r"instituto cervantes|cervantes")
_SP_UNI       = re.compile(r"universidad|universitat|university|\buned\b")
_SP_ELE       = re.compile(r"espa[ñn]ol|spanish|\bele\b|castellano")
# "Многопрофильный" языковой центр — оставляем только если в имени явно указан
# идиоматический маркер на испанском/каталанском/галисийском/баскском.
# Чисто английское "Academy"/"Coulsdon Academy" без слова "idiomas" → отброс.
_SP_GENERIC   = re.compile(
    r"\bidiomas?\b|\blenguas?\b|\bl[ií]nguas?\b|hizkuntza|lleng[uü]es"
)


def classify_spanish(name: str | None) -> str | None:
    """Return short kind code for the spanish layer, or None to drop the row."""
    n = (name or "").lower().strip()
    if not n:
        return None
    # Соофициальные/публичные ловим до DROP — у euskaltegi в названии бывает "english".
    if _SP_EOI.search(n):       return "eoi"
    if _SP_EUSK.search(n):      return "euskaltegi"
    if _SP_CATALAN.search(n):   return "catalan"
    if _SP_GALICIAN.search(n):  return "galician"
    if _SP_DROP.search(n):      return None
    if _SP_CERVANTES.search(n): return "cervantes"
    if _SP_UNI.search(n):       return "university"
    if _SP_ELE.search(n):       return "ele"
    if _SP_GENERIC.search(n):   return "generic"
    return None


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
        if layer_id == "spanish":
            kind = classify_spanish(promoted.get("name"))
            if kind is None:
                continue
            promoted["kind"] = kind
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
