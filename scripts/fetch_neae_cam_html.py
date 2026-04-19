"""
Parse CAM (Comunidad de Madrid) webpage with full lists of schools with
preferential attention for Auditory, Motor, and TEA disabilities.

Source: https://www.comunidad.madrid/servicios/educacion/atencion-preferente-necesidades-educativas-especiales
This page embeds complete lists as HTML tables (better than the PDF snapshots).

Merges Motora + Auditiva + TEA (full list, not just vacancies) into
data/schools_neae.json, replacing the TEA-only snapshot.
"""
from __future__ import annotations
import json
import re
import sys
import time
import subprocess
import unicodedata
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
OSM_SCHOOLS = ROOT / "data" / "osm_schools.json"
OUT_PATH = ROOT / "data" / "schools_neae.json"
CACHE_PATH = ROOT / "data" / "raw" / "neae_geocode_cache.json"
HTML_PATH = ROOT / "data" / "raw" / "cam_neae.html"
SOURCE_URL = "https://www.comunidad.madrid/servicios/educacion/atencion-preferente-necesidades-educativas-especiales"

NOMINATIM = "https://nominatim.openstreetmap.org/search"
UA = "spain-map-relocation/0.1 (curation)"

# Sections in page HTML
SECTION_TYPE_MAP = {
    "Discapacidad Auditiva": "Auditiva",
    "Discapacidad Motora":   "Motora",
    "trastorno del espectro autista": "TEA",
}

MADRID_DISTRITOS = {
    "arganzuela", "barajas", "carabanchel", "centro", "chamartin", "chamberi",
    "ciudad lineal", "fuencarral el pardo", "hortaleza", "latina", "moncloa aravaca",
    "moratalaz", "puente de vallecas", "retiro", "salamanca", "san blas canillejas",
    "tetuan", "usera", "vicalvaro", "villa de vallecas", "villaverde", "moncloa-aravaca",
    "san blas-canillejas",
}


def normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z]+", " ", s.lower()).strip()


def download_html():
    if HTML_PATH.exists() and HTML_PATH.stat().st_size > 10000:
        return
    HTML_PATH.parent.mkdir(parents=True, exist_ok=True)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9",
    }
    r = requests.get(SOURCE_URL, headers=headers, timeout=30)
    r.raise_for_status()
    HTML_PATH.write_text(r.text)
    print(f"[fetch] saved HTML ({r.text.count('<table')} tables)", file=sys.stderr)


# Row pattern after stripping table formatting.
# Format varies, but each row has: distrito, código(7-8 digits), tipo, nombre, etapa
ROW_RE = re.compile(
    r"""^
    (?P<municipio>[A-ZÁÉÍÓÚÑÜ\s,\-\./]+?)\s+
    (?P<codigo>\d{7,8})\s+
    (?P<tipo>EEI|CP\s+INF-PRI-SEC|CP\s+INF-PRI|CP\s+INF|
              CPR\s+INF-PRI-SEC|CPR\s+INF-PRI|CPR\s+INF|
              IES|SIES|ESO)\s+
    (?P<nombre>.+?)\s+
    (?P<etapa>PRIMARIA|SECUNDARIA|INFANTIL|MIXTA|TODAS)
    \s*$
    """,
    re.VERBOSE,
)


def parse_row_text(text: str) -> dict | None:
    m = ROW_RE.match(text)
    if not m:
        return None
    return {
        "codigo": m.group("codigo"),
        "municipio": m.group("municipio").strip(),
        "tipo": m.group("tipo"),
        "nombre": m.group("nombre").strip(),
        "etapa": m.group("etapa"),
    }


def parse_sections_from_html() -> dict[str, list[dict]]:
    """Return {type: [rows]} where type ∈ {TEA, Motora, Auditiva}.

    Format in the HTML: каждая школа — блок из ключ-значение строк:
        DISTRITO, ETAPA EDUCATIVA, CÓDIGO DEL CENTRO, NOMBRE DEL CENTRO,
        DIRECCIÓN DEL CENTRO, TELÉFONO DEL CENTRO.
    """
    soup = BeautifulSoup(HTML_PATH.read_text(), "lxml")
    out: dict[str, list[dict]] = {"TEA": [], "Motora": [], "Auditiva": []}
    tables = soup.find_all("table")

    for t in tables:
        # Figure out disability type from table text
        text = t.get_text(" ", strip=True)[:500].upper()
        if "DISCAPACIDAD AUDITIVA" in text:
            label = "Auditiva"
        elif "DISCAPACIDAD MOTORA" in text or "DISCAPACIDAD MOTÓRICA" in text:
            label = "Motora"
        elif ("TRASTORNO DEL ESPECTRO AUTISTA" in text
              or "TEA" in text or "AUTISMO" in text):
            label = "TEA"
        else:
            continue

        current = {}
        for tr in t.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) < 2:
                # separator row — finalize current record if complete
                if current.get("codigo") and current.get("nombre"):
                    out[label].append(current)
                current = {}
                continue
            key = cells[0].upper().strip()
            val = cells[1].strip()
            if "DISTRITO" in key:
                # If we already have codigo in current, it's a new record starting
                if current.get("codigo") and current.get("nombre"):
                    out[label].append(current)
                    current = {}
                current["municipio"] = val
            elif "ETAPA" in key:
                current["etapa"] = val
            elif "CÓDIGO" in key or "CODIGO" in key:
                current["codigo"] = val
            elif "NOMBRE" in key:
                # Split off "CPR INF-PRI-SEC" prefix from "Colegio X"
                m = re.match(r"^(EEI|CP\s+INF-PRI-SEC|CP\s+INF-PRI|CP\s+INF|"
                             r"CPR\s+INF-PRI-SEC|CPR\s+INF-PRI|CPR\s+INF|"
                             r"IES|SIES|ESO)\s+(.*)$", val, re.I)
                if m:
                    current["tipo"] = m.group(1).upper()
                    current["nombre"] = m.group(2).strip()
                else:
                    current["nombre"] = val
            elif "DIRECC" in key:
                current["address"] = val
            elif "TELÉFONO" in key or "TELEFONO" in key:
                current["phone"] = val
        # flush last
        if current.get("codigo") and current.get("nombre"):
            out[label].append(current)
    return out


# --- Geocoding & enrichment ---

def load_cache():
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text())
    return {}


def save_cache(cache):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=1))


def geocode_query(row: dict) -> str:
    muni_norm = normalize(row.get("municipio", ""))
    city = "Madrid" if muni_norm in MADRID_DISTRITOS else row.get("municipio", "").title()
    prefix = "IES" if row.get("tipo") == "IES" else "Colegio"
    return f"{prefix} {row['nombre'].title()}, {city}, Madrid, Spain"


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
        # Require Madrid CCAA bbox
        if not (39.85 <= lat <= 41.17 and -4.58 <= lng <= -3.05):
            cache[q] = None
            return None
        cache[q] = {"lat": lat, "lng": lng}
        return lat, lng
    except Exception as e:
        print(f"[warn] nominatim: {q[:60]}: {e}", file=sys.stderr)
        return None


SCHOOL_STOPWORDS = {
    "colegio", "centro", "educacion", "infantil", "primaria", "secundaria",
    "instituto", "ies", "ceip", "cp", "cpr", "eei", "escuela",
    "nuestra", "senora", "santa", "san", "santo", "virgen",
    "de", "del", "la", "las", "el", "los", "y", "e", "da", "do",
    "mixta", "publico", "privado", "concertado",
}


def core_tokens(text: str) -> set[str]:
    norm = normalize(text)
    return {t for t in norm.split() if t not in SCHOOL_STOPWORDS and len(t) >= 3}


def enrich_with_osm(lat: float, lng: float, name: str, osm_schools: list) -> dict:
    target = core_tokens(name)
    best = None
    best_j = 0.0
    for f in osm_schools:
        if abs(f["lat"] - lat) > 0.006 or abs(f["lng"] - lng) > 0.007:
            continue
        tokens = core_tokens(f["name"])
        if not tokens or not target:
            continue
        overlap = target & tokens
        if not overlap:
            continue
        j = len(overlap) / len(target | tokens)
        if j > best_j:
            best_j = j
            best = f
    if not best or best_j < 0.3:
        return {}
    return {
        "website": best.get("website"),
        "phone":   best.get("phone"),
        "address": best.get("address"),
    }


def local_osm_fallback(row: dict, osm_schools: list) -> tuple[float, float] | None:
    target = core_tokens(row["nombre"])
    if not target:
        return None
    best = None
    best_j = 0.0
    for f in osm_schools:
        if not (39.85 <= f["lat"] <= 41.17 and -4.58 <= f["lng"] <= -3.05):
            continue
        tokens = core_tokens(f["name"])
        overlap = target & tokens
        if not overlap:
            continue
        if len(overlap) / len(target) < 0.5:
            continue
        j = len(overlap) / len(target | tokens)
        if j > best_j:
            best_j = j
            best = f
    if best and best_j >= 0.5:
        return best["lat"], best["lng"]
    return None


def main() -> int:
    download_html()
    parsed = parse_sections_from_html()
    counts = {k: len(v) for k, v in parsed.items()}
    print(f"[parse] from HTML: {counts}", file=sys.stderr)

    # Merge by código centro: same school can be preferent for multiple types
    merged: dict[str, dict] = {}   # código → row
    for neae_type, rows in parsed.items():
        for r in rows:
            code = r["codigo"]
            if code not in merged:
                merged[code] = {**r, "neae_types": set()}
            merged[code]["neae_types"].add(neae_type)

    print(f"[merge] unique schools: {len(merged)}", file=sys.stderr)

    cache = load_cache()
    osm_data = json.loads(OSM_SCHOOLS.read_text())
    osm_schools = osm_data["features"]

    features = []
    unmatched = []
    for code, r in merged.items():
        q = geocode_query(r)
        # Prefer to geocode using full address (we have it from HTML!)
        address_q = None
        if r.get("address"):
            address_q = f"{r['address']}, Madrid, Spain"
            addr_coords = geocode(address_q, cache)
            if addr_coords:
                coords = addr_coords
            else:
                coords = geocode(q, cache)
        else:
            coords = geocode(q, cache)
        if not coords:
            coords = local_osm_fallback(r, osm_schools)
        if not coords:
            unmatched.append(r)
            continue
        lat, lng = coords
        # Fall back to OSM only for website (HTML already gives address/phone)
        enrichment = enrich_with_osm(lat, lng, r["nombre"], osm_schools)
        rec = {
            "codigo": code,
            "name": r["nombre"],
            "municipio": r.get("municipio", ""),
            "etapa": r.get("etapa", ""),
            "tipo": r.get("tipo", ""),
            "lat": lat,
            "lng": lng,
            "neae_types": sorted(r["neae_types"]),
            "ccaa": "Madrid",
            "source": "CAM — centros preferentes (full HTML lists)",
        }
        # Data from CAM HTML (authoritative)
        if r.get("address"):
            rec["address"] = r["address"]
        if r.get("phone"):
            rec["phone"] = r["phone"]
        # Website only from OSM enrichment
        if enrichment.get("website"):
            rec["website"] = enrichment["website"]
        features.append(rec)
        if len(features) % 50 == 0:
            save_cache(cache)
            print(f"[geo] processed {len(features)}/{len(merged)} "
                  f"(cache {len(cache)})", file=sys.stderr)
    save_cache(cache)

    print(f"[done] {len(features)} geocoded; {len(unmatched)} unresolved", file=sys.stderr)

    payload = {
        "source": "CAM — centros preferentes NEAE (Auditiva + Motora + TEA)",
        "source_url": SOURCE_URL,
        "period": "2024-25",
        "count": len(features),
        "unmatched_count": len(unmatched),
        "breakdown": {t: sum(1 for f in features if t in f["neae_types"]) for t in ("TEA", "Motora", "Auditiva")},
        "features": features,
    }
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[ok] Wrote {OUT_PATH} ({size_kb:.1f} KB, {len(features)} schools); "
          f"breakdown={payload['breakdown']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
