"""
Parse Valencia DOGV annex PDF listing schools with specific units
(unidades específicas: Comunicació i llenguatge, Mixta, UECO).

Source:
  DOGV Resolución 7 marzo 2022 (2022_1942) — creación de unidades específicas
  https://anpecomunidadvalenciana.es/openFile.php?link=notices/att/4/2022_1942_t1646816861_4_1.pdf

Row format:
  <CODI 8-digit> <Tipo CEIP/CEE/IES> <NOMBRE multi-word> <LOCALITAT> <Tipus d'unitat>

Output: merged into data/schools_neae.json with ccaa='Valenciana' and
neae_types derived from unit type:
  'Comunicació i llenguatge' → Lenguaje
  'Mixta' → TEA (mixta = TEA + lenguaje)
  'UECO' → TEA
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
PDF_PATH = ROOT / "data" / "raw" / "valencia_cyl_2022.pdf"
OUT_PATH = ROOT / "data" / "schools_neae.json"
CACHE_PATH = ROOT / "data" / "raw" / "valencia_geocode_cache.json"
SOURCE_URL = "https://dogv.gva.es/"

NOMINATIM = "https://nominatim.openstreetmap.org/search"
UA = "spain-map-relocation/0.1 (curation)"

# Valencia CCAA bbox (3 provinces: Alicante, Castellón, Valencia)
VALENCIA_BBOX = (37.84, -1.55, 40.79, 0.78)


def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z]+", " ", s.lower()).strip()


# Line format examples:
#   03001507 CEIP Pedro Duque Alacant Comunicació i llenguatge
#   12006810 CEIP Regina Violant Almassora Comunicació i llenguatge
#   46017200 IES Benicalap València UECO
#   46000061 CEE Nuestra Senyora del Carmen Valencia UECO

ROW_RE = re.compile(
    r"""^
    (?P<codi>\d{8})\s+
    (?P<tipo>CEIP|CEE|IES|CEIPS)\s+
    (?P<nombre>.+?)\s+
    (?P<tipus>Comunicaci[oó].*?llenguatge|Mixta|UECO|UEE|CyL)
    \s*$
    """,
    re.VERBOSE | re.IGNORECASE,
)


def parse_pdf() -> list[dict]:
    """Extract (codi, tipo_centro, nombre_centro, localitat, tipus_unitat)."""
    out = []
    current_province = None
    with pdfplumber.open(PDF_PATH) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                # Province headers shift localitat context
                if "província d'Alacant" in line or "provincia de Alicante" in line:
                    current_province = "Alicante"
                    continue
                if "província de Castelló" in line or "provincia de Castellón" in line:
                    current_province = "Castellón"
                    continue
                if "província de València" in line or "provincia de Valencia" in line:
                    current_province = "Valencia"
                    continue
                m = ROW_RE.match(line)
                if not m:
                    continue
                nombre_raw = m.group("nombre").strip()
                # Split: "Pedro Duque Alacant" — last word(s) is locality
                # Heuristic: the locality is everything after the last school-name word
                # Since we can't split reliably, we'll rely on Nominatim for geocoding
                # and keep the raw string.
                out.append({
                    "codi": m.group("codi"),
                    "tipo_centro": m.group("tipo").upper(),
                    "name_raw": nombre_raw,
                    "tipus_unitat": m.group("tipus"),
                    "province": current_province,
                })
    return out


def split_name_locality(raw: str) -> tuple[str, str]:
    """Heuristic: try to split "School Name Localidad" when localidad is 1-3 words.
    We don't have strong markers; treat the last word as locality unless it's
    part of common name suffixes. Good enough for geocoding which is lenient."""
    words = raw.split()
    if len(words) < 3:
        return raw, ""
    # try last 2 then last 1 word as locality
    localidad = words[-1]
    name = " ".join(words[:-1])
    return name, localidad


def unit_to_neae_types(tipus: str) -> list[str]:
    t = tipus.lower()
    if "comunic" in t:
        return ["Lenguaje"]
    if "mixta" in t:
        return ["TEA", "Lenguaje"]
    if "ueco" in t or "uee" in t:
        return ["TEA"]
    return [tipus]


def load_cache():
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text())
    return {}


def save_cache(cache):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=1))


def geocode(query: str, cache: dict) -> tuple[float, float] | None:
    if query in cache:
        v = cache[query]
        return (v["lat"], v["lng"]) if v else None
    try:
        r = requests.get(
            NOMINATIM,
            params={"q": query, "format": "json", "limit": 1, "countrycodes": "es"},
            headers={"User-Agent": UA, "Accept-Language": "es,ca,en"},
            timeout=20,
        )
        time.sleep(1.5)
        if r.status_code != 200 or not r.json():
            cache[query] = None
            return None
        hit = r.json()[0]
        lat, lng = float(hit["lat"]), float(hit["lon"])
        if not (VALENCIA_BBOX[0] <= lat <= VALENCIA_BBOX[2]
                and VALENCIA_BBOX[1] <= lng <= VALENCIA_BBOX[3]):
            cache[query] = None
            return None
        cache[query] = {"lat": lat, "lng": lng}
        return lat, lng
    except Exception as e:
        print(f"[warn] nominatim: {query[:60]}: {e}", file=sys.stderr)
        return None


def main() -> int:
    rows = parse_pdf()
    print(f"[parse] {len(rows)} Valencia NEAE rows", file=sys.stderr)

    # Merge by codi
    by_codi: dict[str, dict] = {}
    for r in rows:
        neae = unit_to_neae_types(r["tipus_unitat"])
        if r["codi"] not in by_codi:
            by_codi[r["codi"]] = {**r, "neae_types": set(neae)}
        else:
            by_codi[r["codi"]]["neae_types"].update(neae)

    cache = load_cache()
    features = []
    unmatched = []
    for codi, r in by_codi.items():
        name, locality = split_name_locality(r["name_raw"])
        queries = [
            f"{r['tipo_centro']} {name}, {locality}, Valencia, Spain",
            f"{name}, {locality}, {r['province'] or 'Valencia'}, Spain",
            f"{name}, {r['province'] or 'Valencia'}, Spain",
        ]
        coords = None
        for q in queries:
            coords = geocode(q, cache)
            if coords:
                break
        if not coords:
            unmatched.append(r)
            continue
        lat, lng = coords
        features.append({
            "codigo": codi,
            "name": name,
            "municipio": locality,
            "etapa": "INFANTIL-PRIMARIA",
            "tipo": r["tipo_centro"],
            "lat": lat,
            "lng": lng,
            "neae_types": sorted(r["neae_types"]),
            "ccaa": "Valenciana",
            "source": f"DOGV — unidades específicas ({r['tipus_unitat']})",
        })
        if len(features) % 25 == 0:
            save_cache(cache)
            print(f"[geo] {len(features)}/{len(by_codi)}", file=sys.stderr)
    save_cache(cache)

    print(f"[done] {len(features)} Valencia geocoded; {len(unmatched)} unresolved",
          file=sys.stderr)

    # Merge into existing Madrid NEAE file
    existing = json.loads(OUT_PATH.read_text()) if OUT_PATH.exists() else {"features": []}
    madrid_features = [f for f in existing.get("features", []) if f.get("ccaa") == "Madrid"]
    merged = madrid_features + features

    payload = {
        "source": "CAM (Madrid, full HTML lists) + DOGV (Valencia, Resolución 2022_1942)",
        "source_urls": [
            "https://www.comunidad.madrid/servicios/educacion/atencion-preferente-necesidades-educativas-especiales",
            "https://anpecomunidadvalenciana.es/openFile.php?link=notices/att/4/2022_1942_t1646816861_4_1.pdf",
        ],
        "count": len(merged),
        "breakdown_by_ccaa": {
            "Madrid": len(madrid_features),
            "Valenciana": len(features),
        },
        "features": merged,
    }
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[ok] Merged → {OUT_PATH.name} ({size_kb:.0f} KB, {len(merged)} schools)",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
