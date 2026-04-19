"""
Parse Murcia 'Aulas Abiertas Especializadas' PDF — schools with specialized
open classrooms (equivalent to Madrid's TEA/Motora units).

Source: https://www.carm.es/web/descarga?ARCHIVO=aa_2023_2024.pdf&ALIAS=ARCH&IDCONTENIDO=124545
  RELACIÓN DE AULAS ABIERTAS ESPECIALIZADAS EN CENTROS PÚBLICOS Y PRIVADOS
  CONCERTADOS — Curso 2025-2026

Output: merged into data/schools_neae.json with ccaa='Murcia'.
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
PDF_PATH = ROOT / "data" / "raw" / "murcia_aa_2024.pdf"
OUT_PATH = ROOT / "data" / "schools_neae.json"
CACHE_PATH = ROOT / "data" / "raw" / "murcia_geocode_cache.json"
SOURCE_URL = "https://www.carm.es/web/pagina?IDCONTENIDO=53499&IDTIPO=100"

NOMINATIM = "https://nominatim.openstreetmap.org/search"
UA = "spain-map-relocation/0.1 (curation)"

# Región de Murcia bbox
MURCIA_BBOX = (37.33, -2.35, 38.78, -0.59)

# Matches: CODE TYPE NAME LOCALITY [Inf] [Sec] TOTAL
# Accept codes with or without a leading 30 prefix (Murcia's province code).
ROW_RE = re.compile(
    r"""
    ^\s*
    (?P<codigo>\d{8})\s+
    (?P<tipo>CEIP|CEE|IES|CC)\s+
    (?P<tail>.+)$
    """,
    re.VERBOSE,
)


def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z]+", " ", s.lower()).strip()


def parse_tail(tail: str) -> tuple[str, str]:
    """Split 'NAME ... LOCALITY N1 N2 TOTAL' → (name, locality).

    LOCALITY is all-uppercase ASCII (allows accented letters) at the boundary
    before the trailing counts. NAME is everything before it.
    """
    # drop trailing counts (sequence of integers possibly with blanks)
    m = re.match(r"^(.+?)\s+(\d+(?:\s+\d+)*)\s*$", tail)
    if not m:
        return tail.strip(), ""
    lead = m.group(1).strip()
    # locality = last token(s) that are all-uppercase
    tokens = lead.split()
    # walk from the end, collect tokens while all-uppercase (allowing accents)
    loc = []
    while tokens:
        t = tokens[-1]
        # Allow multi-word localities like "ALHAMA DE MURCIA" — stop at mixed case
        if re.fullmatch(r"[A-ZÁÉÍÓÚÜÑ\.\-]+", t) or t.upper() in {"DE", "LA", "LAS", "EL", "LOS"}:
            loc.insert(0, t)
            tokens.pop()
        else:
            break
    name = " ".join(tokens)
    locality = " ".join(loc).title()
    return name.strip(), locality


def parse_pdf() -> list[dict]:
    """Extract schools using pdfplumber's table cells — each row yields
    [-, CODE, -, -, TYPE, NAME, -, -, LOCALITY, N1, (N2), TOTAL] with varying gaps."""
    out = []
    with pdfplumber.open(PDF_PATH) as pdf:
        for page in pdf.pages:
            for tbl in page.extract_tables():
                for row in tbl:
                    # Collect non-empty cells
                    cells = [c.strip() for c in row if c and c.strip()]
                    if len(cells) < 4:
                        continue
                    # Must start with 8-digit code
                    if not re.fullmatch(r"\d{8}", cells[0]):
                        continue
                    if cells[1] not in ("CEIP", "CEE", "IES", "CC"):
                        continue
                    # Filter: last 1-3 cells are numeric counts
                    i = len(cells) - 1
                    counts = []
                    while i > 0 and re.fullmatch(r"\d+", cells[i]):
                        counts.insert(0, cells[i])
                        i -= 1
                    if not counts:
                        continue
                    # Between TYPE and counts: name + locality
                    middle = cells[2:i+1]
                    if len(middle) < 2:
                        continue
                    # locality is the last element, name is the joined rest
                    locality = middle[-1]
                    name = " ".join(middle[:-1])
                    out.append({
                        "codigo": cells[0],
                        "tipo": cells[1],
                        "name_raw": name,
                        "municipio": locality.title(),
                    })
    return out


def load_cache():
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text())
    return {}


def save_cache(cache):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=1))


def geocode(q: str, cache: dict) -> tuple[float, float] | None:
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
        if not (MURCIA_BBOX[0] <= lat <= MURCIA_BBOX[2]
                and MURCIA_BBOX[1] <= lng <= MURCIA_BBOX[3]):
            cache[q] = None
            return None
        cache[q] = {"lat": lat, "lng": lng}
        return lat, lng
    except Exception as e:
        print(f"[warn] {q[:60]}: {e}", file=sys.stderr)
        return None


def main() -> int:
    rows = parse_pdf()
    print(f"[parse] {len(rows)} Murcia schools", file=sys.stderr)

    cache = load_cache()
    features = []
    unmatched = []
    for r in rows:
        queries = [
            f"{r['tipo']} {r['name_raw'].title()}, {r['municipio']}, Murcia, Spain",
            f"{r['name_raw'].title()}, {r['municipio']}, Murcia, Spain",
            f"{r['name_raw'].title()}, Murcia, Spain",
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
            "codigo": r["codigo"],
            "name": r["name_raw"].title(),
            "municipio": r["municipio"],
            "etapa": "INFANTIL-PRIMARIA-SECUNDARIA",
            "tipo": r["tipo"],
            "lat": lat,
            "lng": lng,
            "neae_types": ["AA"],  # aula abierta especializada — TEA/severe NEAE
            "ccaa": "Murcia",
            "source": "CARM — Aulas Abiertas Especializadas 2025-2026",
        })
        if len(features) % 20 == 0:
            save_cache(cache)
            print(f"[geo] {len(features)}/{len(rows)}", file=sys.stderr)
    save_cache(cache)

    print(f"[done] {len(features)} Murcia geocoded; {len(unmatched)} unresolved",
          file=sys.stderr)

    existing = json.loads(OUT_PATH.read_text()) if OUT_PATH.exists() else {"features": []}
    keep = [f for f in existing.get("features", []) if f.get("ccaa") != "Murcia"]
    merged = keep + features

    payload = {
        "source": "CAM Madrid + Valencia DOGV + Andalucía Junta + Murcia CARM",
        "count": len(merged),
        "breakdown_by_ccaa": {
            ccaa: sum(1 for f in merged if f.get("ccaa") == ccaa)
            for ccaa in ("Madrid", "Valenciana", "Andalucía", "Murcia")
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
