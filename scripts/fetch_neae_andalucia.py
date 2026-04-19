"""
Extract Andalucía schools offering Educación Especial (NEAE support)
from the Junta de Andalucía open data directory CSV.

Source:
  https://www.juntadeandalucia.es/datosabiertos/portal/dataset/directorio-de-centros-docentes-de-andalucia
  CSV includes per-school flags pub_ee / priv_c_ee / priv_noc_ee = 'S'|'N'
  indicating whether the centre offers EE (Educación Especial) units.

Note: Andalucía's open data doesn't break EE down by TEA/Motora/Auditiva —
just a generic flag. We label all matched schools with neae_types=['EE'].

Output: merged into data/schools_neae.json with existing Madrid + Valencia data.
"""
from __future__ import annotations
import csv
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / "data" / "raw" / "andalucia_centros.csv"
OUT_PATH = ROOT / "data" / "schools_neae.json"
SOURCE_URL = "https://www.juntadeandalucia.es/datosabiertos/portal/dataset/directorio-de-centros-docentes-de-andalucia"


def num(s: str) -> float | None:
    if not s:
        return None
    s = s.strip().replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def clean_name(d_denomina: str, d_especifica: str) -> str:
    """Normalize 'Colegio de Educación Infantil y Primaria' + specific name."""
    name = (d_especifica or "").strip().strip('"').title()
    if not name:
        name = (d_denomina or "").strip()
    return name


def infer_etapa(row: dict) -> str:
    parts = []
    if any(row.get(k) == "S" for k in ("pub_inf2", "priv_c_inf2", "priv_noc_inf2")):
        parts.append("INFANTIL")
    if any(row.get(k) == "S" for k in ("pub_pri", "priv_c_pri", "priv_noc_pri")):
        parts.append("PRIMARIA")
    if any(row.get(k) == "S" for k in ("pub_eso", "priv_c_eso", "priv_noc_eso")):
        parts.append("SECUNDARIA")
    return "-".join(parts) or ""


def extract_ee(row: dict) -> bool:
    return any(row.get(k) == "S" for k in ("pub_ee", "priv_c_ee", "priv_noc_ee"))


def main() -> int:
    features = []
    skipped_no_coords = 0
    with CSV_PATH.open(encoding="utf-8") as f:
        r = csv.DictReader(f, delimiter=";", quotechar='"')
        for row in r:
            if not extract_ee(row):
                continue
            lat = num(row.get("N_LATITUD"))
            lng = num(row.get("N_LONGITUD"))
            if lat is None or lng is None:
                skipped_no_coords += 1
                continue
            name = clean_name(row.get("D_DENOMINA", ""), row.get("D_ESPECIFICA", ""))
            features.append({
                "codigo": row["codigo"].strip().strip('"'),
                "name": name,
                "municipio": row.get("D_MUNICIPIO", "").strip(),
                "etapa": infer_etapa(row),
                "tipo": (row.get("D_TIPO") or "").strip(),
                "lat": lat,
                "lng": lng,
                "address": row.get("D_DOMICILIO", "").strip() or None,
                "phone": row.get("N_TELEFONO", "").strip() or None,
                "email": row.get("Correo_e", "").strip() or None,
                "neae_types": ["EE"],   # binary flag in source
                "ccaa": "Andalucía",
                "source": "Junta de Andalucía — Directorio de centros docentes (flag pub_ee/priv_ee)",
            })
    print(f"[parse] {len(features)} Andalucía schools with EE "
          f"(skipped {skipped_no_coords} without coords)", file=sys.stderr)

    # Merge with existing
    existing = json.loads(OUT_PATH.read_text()) if OUT_PATH.exists() else {"features": []}
    keep_features = [f for f in existing.get("features", []) if f.get("ccaa") != "Andalucía"]
    merged = keep_features + features

    payload = {
        "source": "CAM Madrid (HTML lists) + Valencia DOGV 2022_1942 + Andalucía Junta Directorio",
        "source_urls": [
            "https://www.comunidad.madrid/servicios/educacion/atencion-preferente-necesidades-educativas-especiales",
            "https://anpecomunidadvalenciana.es/openFile.php?link=notices/att/4/2022_1942_t1646816861_4_1.pdf",
            SOURCE_URL,
        ],
        "count": len(merged),
        "breakdown_by_ccaa": {
            "Madrid":     sum(1 for f in merged if f.get("ccaa") == "Madrid"),
            "Valenciana": sum(1 for f in merged if f.get("ccaa") == "Valenciana"),
            "Andalucía":  sum(1 for f in merged if f.get("ccaa") == "Andalucía"),
        },
        "features": merged,
    }
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[ok] Merged → {OUT_PATH.name} ({size_kb:.0f} KB, {len(merged)} schools); "
          f"breakdown={payload['breakdown_by_ccaa']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
