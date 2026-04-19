"""
Parse Castilla-La Mancha TEA schools PDF.

Source: JCCM Anexo C — Centros con Aulas Abiertas Especializadas y
Equipos de Atención Educativa TEA, curso 2025-2026.
https://www.jccm.es/tramites/descarga/25656/1016557_25656.PDF

Format per row: "<Provincia> <Tipo> <Nombre...> <Localidad> <Código>"
Two sections:
  I.  AULAS ABIERTAS ESPECIALIZADAS TEA      → label 'AA'
  II. EQUIPOS DE ATENCIÓN EDUCATIVA TEA       → label 'EAE'

Output: merged into data/schools_neae.json with ccaa='Castilla-La Mancha'.
"""
from __future__ import annotations
import json
import re
import sys
import time
import unicodedata
from pathlib import Path

import pdfplumber
import requests

ROOT = Path(__file__).resolve().parent.parent
PDF_PATH = ROOT / "data" / "raw" / "clm_tea_2025.pdf"
OUT_PATH = ROOT / "data" / "schools_neae.json"
CACHE_PATH = ROOT / "data" / "raw" / "clm_geocode_cache.json"
SOURCE_URL = "https://www.jccm.es/tramites/descarga/25656/1016557_25656.PDF"

NOMINATIM = "https://nominatim.openstreetmap.org/search"
UA = "spain-map-relocation/0.1 (curation)"

CLM_BBOX = (38.35, -5.40, 41.30,  -1.00)   # approx CLM bounds (excl. Albacete extreme east)
# Actually Albacete extends to ~-0.87
CLM_BBOX = (37.85, -5.45, 41.35, -0.80)

PROVINCES = {"Albacete", "Ciudad Real", "Cuenca", "Guadalajara", "Toledo"}
TIPOS = r"CEIP|CEIPSO|CRA|IES|IESO|CEE|CC"

# Build the regex with province alternation
PROV_ALT = "|".join(re.escape(p) for p in PROVINCES)
ROW_RE = re.compile(
    rf"""^
    (?P<provincia>{PROV_ALT})\s+
    (?P<tipo>{TIPOS})\s+
    (?P<middle>.+?)\s+
    (?P<codigo>\d{{7,8}})
    \s*$""",
    re.VERBOSE
)


def normalize(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z]+", " ", s.lower()).strip()


# Известные локалидады Castilla-La Mancha — чтобы правильно отделить имя от муниципио.
KNOWN_LOCALITIES = {
    normalize(x) for x in (
        "Albacete", "Hellín", "Casas Ibáñez", "Villamalea", "Villarrobledo",
        "Alcázar de San Juan", "Ciudad Real", "Daimiel", "Manzanares",
        "Miguelturra", "Pedro Muñoz", "Puertollano", "Tomelloso",
        "Valdepeñas", "Cuenca", "Horcajo de Santiago", "Las Pedroñeras",
        "Motilla del Palancar", "Motilla de Palancar", "Tarancón", "Pioz",
        "Azuqueca de Henares", "Guadalajara", "Yebes", "Añover de Tajo",
        "Cedillo del Condado", "Chozas de Canales", "Consuegra", "Illescas",
        "Mora", "Nambroca", "Ocaña", "Recas", "Talavera de la Reina",
        "Toledo", "Ugena", "Villafranca de los Caballeros", "Yepes",
        "Yuncler", "Pepino",
    )
}


def split_name_locality(middle: str) -> tuple[str, str]:
    """Take longest suffix that matches a known locality."""
    words = middle.split()
    for start in range(len(words)):
        candidate = " ".join(words[start:])
        if normalize(candidate) in KNOWN_LOCALITIES:
            name = " ".join(words[:start])
            if name:
                return name, candidate
    # Fallback: last word is locality
    if len(words) >= 2:
        return " ".join(words[:-1]), words[-1]
    return middle, ""


def parse_pdf() -> list[dict]:
    rows = []
    section_label = "AA"   # default first section
    with pdfplumber.open(PDF_PATH) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw in text.splitlines():
                line = raw.strip()
                if "II. CENTROS CON EQUIPOS" in line.upper():
                    section_label = "EAE"
                if "I. CENTROS CON AULAS" in line.upper():
                    section_label = "AA"
                m = ROW_RE.match(line)
                if not m:
                    continue
                name, locality = split_name_locality(m.group("middle").strip())
                rows.append({
                    "codigo": m.group("codigo"),
                    "tipo": m.group("tipo"),
                    "name_raw": name,
                    "municipio": locality,
                    "provincia": m.group("provincia"),
                    "section": section_label,
                })
    return rows


def load_cache():
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text())
    return {}


def save_cache(cache):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=1))


def geocode(q, cache):
    if q in cache:
        v = cache[q]
        return (v["lat"], v["lng"]) if v else None
    try:
        r = requests.get(
            NOMINATIM,
            params={"q": q, "format": "json", "limit": 1, "countrycodes": "es"},
            headers={"User-Agent": UA, "Accept-Language": "es,en"},
            timeout=20,
        )
        time.sleep(1.5)
        if r.status_code != 200 or not r.json():
            cache[q] = None
            return None
        hit = r.json()[0]
        lat, lng = float(hit["lat"]), float(hit["lon"])
        if not (CLM_BBOX[0] <= lat <= CLM_BBOX[2] and CLM_BBOX[1] <= lng <= CLM_BBOX[3]):
            cache[q] = None
            return None
        cache[q] = {"lat": lat, "lng": lng}
        return lat, lng
    except Exception as e:
        print(f"[warn] {q[:60]}: {e}", file=sys.stderr)
        return None


def main() -> int:
    rows = parse_pdf()
    print(f"[parse] {len(rows)} CLM TEA schools", file=sys.stderr)

    cache = load_cache()
    features = []
    unmatched = []
    for r in rows:
        queries = [
            f"{r['tipo']} {r['name_raw']}, {r['municipio']}, {r['provincia']}, Spain",
            f"{r['name_raw']}, {r['municipio']}, {r['provincia']}, Spain",
            f"{r['name_raw']}, {r['provincia']}, Spain",
        ]
        coords = None
        for q in queries:
            coords = geocode(q, cache)
            if coords: break
        if not coords:
            unmatched.append(r)
            continue
        lat, lng = coords
        features.append({
            "codigo": r["codigo"],
            "name": r["name_raw"],
            "municipio": r["municipio"],
            "provincia": r["provincia"],
            "etapa": "INFANTIL-PRIMARIA-SECUNDARIA",
            "tipo": r["tipo"],
            "lat": lat, "lng": lng,
            "neae_types": ["TEA"] + (["EAE"] if r["section"] == "EAE" else ["AA"]),
            "ccaa": "Castilla-La Mancha",
            "source": "JCCM — Anexo C Aulas TEA 2025-2026",
        })
        if len(features) % 20 == 0:
            save_cache(cache)
            print(f"[geo] {len(features)}/{len(rows)}", file=sys.stderr)
    save_cache(cache)
    print(f"[done] {len(features)} CLM geocoded; {len(unmatched)} unresolved", file=sys.stderr)

    existing = json.loads(OUT_PATH.read_text()) if OUT_PATH.exists() else {"features": []}
    keep = [f for f in existing.get("features", []) if f.get("ccaa") != "Castilla-La Mancha"]
    merged = keep + features
    payload = {
        "source": "CAM + DOGV Valencia + Andalucía + CARM Murcia + JCCM Castilla-La Mancha",
        "count": len(merged),
        "breakdown_by_ccaa": {
            ccaa: sum(1 for f in merged if f.get("ccaa") == ccaa)
            for ccaa in ("Madrid", "Valenciana", "Andalucía", "Murcia", "Castilla-La Mancha")
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
