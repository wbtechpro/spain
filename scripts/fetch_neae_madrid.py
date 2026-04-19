"""
Parse Madrid's PDF listing schools with TEA (autism) preferential attention.
Match each school against our OSM school snapshot to get coordinates.

Source: https://www.comunidad.madrid/sites/default/files/informacion_centros_preferentes_tea_curso_2024-25.pdf
Note: the PDF lists centres with OPEN places (< 5 students enrolled), not all
682 TEA-preferential centres. For a full picture we'd need the CAM internal
registry. This gives us confirmed-positive TEA centres in Madrid CCAA.

Output merged into data/schools_neae.json (one record per school with NEAE flags).
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
PDF_PATH = ROOT / "data" / "raw" / "madrid_tea.pdf"
OSM_SCHOOLS = ROOT / "data" / "osm_schools.json"
OUT_PATH = ROOT / "data" / "schools_neae.json"
CACHE_PATH = ROOT / "data" / "raw" / "neae_geocode_cache.json"

NOMINATIM = "https://nominatim.openstreetmap.org/search"
# Polite contact per Nominatim usage policy
NOMINATIM_UA = "spain-map-relocation/0.1 (github.com personal project)"

# Distritos внутри Мадрид-города — относятся к "Madrid" для геокодинга.
MADRID_DISTRITOS = {
    "arganzuela", "barajas", "carabanchel", "centro", "chamartin", "chamberi",
    "ciudad lineal", "fuencarral el pardo", "hortaleza", "latina", "moncloa aravaca",
    "moratalaz", "puente de vallecas", "retiro", "salamanca", "san blas canillejas",
    "tetuan", "usera", "vicalvaro", "villa de vallecas", "villaverde",
    "a carabanchel",  # встречающаяся опечатка OCR
}

# Row pattern for TEA centre entries. Examples:
#   ARGANZUELA 28010722 CP INF-PRI JOAQUÍN COSTA PRIMARIA
#   CHAMBERÍ 28010618 CP INF-PRI FERNANDO EL CATÓLICO PRIMARIA
#   CARABANCHEL 28073151 CPR INF-PRI-SEC COLEGIO ARENALES DE CARABANCHEL PRIMARIA
# The centre code (`codigo`) is a 7-8 digit Spanish centre ID.
ROW_RE = re.compile(
    r"""^
    (?P<municipio>[A-ZÁÉÍÓÚÑÜ\s,\-\./]+?)\s+
    (?P<codigo>\d{7,8})\s+
    (?P<tipo>CP\s+INF-PRI-SEC|CP\s+INF-PRI|CP\s+INF|
              CPR\s+INF-PRI-SEC|CPR\s+INF-PRI|CPR\s+INF|
              IES|ESO)\s+
    (?P<nombre>.+?)\s+
    (?P<etapa>PRIMARIA|SECUNDARIA|INFANTIL|MIXTA)
    \s*$
    """,
    re.VERBOSE,
)


def normalize(s: str) -> str:
    """Lowercase and strip accents/diacritics for fuzzy name matching."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def parse_pdf(pdf_path: Path) -> list[dict]:
    rows = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            # PDFs sometimes insert a column marker "L A T I N A" rotated on page
            # edges; that produces one-letter lines we want to ignore.
            for line in txt.splitlines():
                line = line.strip()
                if not line or len(line) < 20:
                    continue
                # Skip rotated-letter junk (single letters separated by spaces)
                if re.match(r"^([A-Z]\s?){3,}$", line):
                    continue
                m = ROW_RE.match(line)
                if not m:
                    continue
                rows.append({
                    "codigo": m.group("codigo"),
                    "municipio": m.group("municipio").strip(),
                    "tipo": m.group("tipo"),
                    "nombre": m.group("nombre").strip(),
                    "etapa": m.group("etapa"),
                })
    return rows


def load_cache() -> dict:
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text())
    return {}


def save_cache(cache: dict):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=1))


def geocode_key(row: dict) -> str:
    muni_norm = re.sub(r"[^a-z]+", " ", (row["municipio"] or "").lower()).strip()
    city = "Madrid" if muni_norm in MADRID_DISTRITOS else row["municipio"].title()
    # Prefix with school-type hint so Nominatim finds the POI
    prefix = "IES" if row["tipo"] == "IES" else "Colegio"
    return f"{prefix} {row['nombre'].title()}, {city}, Madrid, Spain"


# Стопслова для испанских школ. Удаляем перед сравнением токенов.
SCHOOL_STOPWORDS = {
    "colegio", "centro", "educacion", "infantil", "primaria", "secundaria",
    "instituto", "ies", "ceip", "cp", "cpr", "escuela", "escola", "nuestra",
    "de", "del", "la", "las", "el", "los", "y", "e", "da", "do",
    "mixta", "publico", "privado", "concertado",
    "santa", "san", "santo", "senora", "senora", "virgen",  # слишком частые
}


def core_tokens(text: str) -> set[str]:
    norm = unicodedata.normalize("NFKD", text or "")
    norm = "".join(c for c in norm if not unicodedata.combining(c)).lower()
    tokens = re.findall(r"[a-z0-9]+", norm)
    return {t for t in tokens if t not in SCHOOL_STOPWORDS and len(t) >= 3}


# Bbox Comunidad de Madrid
MADRID_BBOX = (39.85, -4.58, 41.17, -3.05)


def in_madrid(lat, lng):
    return MADRID_BBOX[0] <= lat <= MADRID_BBOX[2] and MADRID_BBOX[1] <= lng <= MADRID_BBOX[3]


def local_osm_fallback(row: dict) -> tuple[float, float] | None:
    """Поиск по локальному osm_schools.json, если Nominatim не нашёл.
    Требуем сильного пересечения distinctive-токенов."""
    if not hasattr(local_osm_fallback, "_cache"):
        data = json.loads(OSM_SCHOOLS.read_text())
        filtered = [
            {"name": f["name"], "lat": f["lat"], "lng": f["lng"],
             "tokens": core_tokens(f["name"])}
            for f in data["features"]
            if in_madrid(f["lat"], f["lng"])
        ]
        local_osm_fallback._cache = filtered
    osm = local_osm_fallback._cache
    target = core_tokens(row["nombre"])
    if len(target) < 1:
        return None
    best = None
    best_score = 0.0
    for s in osm:
        if not s["tokens"]:
            continue
        overlap = target & s["tokens"]
        if not overlap:
            continue
        # Jaccard similarity on distinctive tokens
        union = target | s["tokens"]
        j = len(overlap) / len(union)
        # Require intersection covers at least half of target tokens
        if len(overlap) / len(target) < 0.5:
            continue
        if j > best_score:
            best_score = j
            best = s
    if best and best_score >= 0.5:
        return best["lat"], best["lng"]
    return None


def geocode(q: str, cache: dict) -> tuple[float, float] | None:
    if q in cache:
        v = cache[q]
        return (v["lat"], v["lng"]) if v else None
    try:
        r = requests.get(
            NOMINATIM,
            params={"q": q, "format": "json", "limit": 1, "countrycodes": "es"},
            headers={"User-Agent": NOMINATIM_UA, "Accept-Language": "es,en"},
            timeout=20,
        )
        time.sleep(1.5)   # Nominatim policy: ≤1 req/s; extra buffer против 429
        if r.status_code != 200:
            print(f"[warn] nominatim HTTP {r.status_code} for {q!r}", file=sys.stderr)
            cache[q] = None
            return None
        results = r.json()
        if not results:
            cache[q] = None
            return None
        hit = results[0]
        lat, lng = float(hit["lat"]), float(hit["lon"])
        # Require result to be inside Madrid CCAA bbox
        if not (39.85 <= lat <= 41.17 and -4.58 <= lng <= -3.05):
            cache[q] = None
            return None
        cache[q] = {"lat": lat, "lng": lng}
        return lat, lng
    except Exception as e:
        print(f"[warn] nominatim error for {q!r}: {e}", file=sys.stderr)
        return None


def main() -> int:
    if not PDF_PATH.exists():
        print(f"[error] PDF not found: {PDF_PATH}", file=sys.stderr)
        return 1
    rows = parse_pdf(PDF_PATH)
    print(f"[parse] {len(rows)} rows from PDF", file=sys.stderr)

    cache = load_cache()
    # Загружаем полный OSM-слой школ для последующего обогащения
    osm_data = json.loads(OSM_SCHOOLS.read_text())
    osm_schools_all = osm_data["features"]

    def enrich_from_osm(lat: float, lng: float, name: str) -> dict:
        """Ищем OSM-школу в радиусе ~500м с похожим именем, забираем контакты."""
        target_tokens = core_tokens(name)
        best = None
        best_score = 0.0
        for f in osm_schools_all:
            # Грубое расстояние — 0.005° ≈ 500м по широте
            dlat = abs(f["lat"] - lat)
            dlng = abs(f["lng"] - lng)
            if dlat > 0.006 or dlng > 0.007:
                continue
            osm_tokens = core_tokens(f["name"])
            if not osm_tokens or not target_tokens:
                continue
            overlap = target_tokens & osm_tokens
            if not overlap:
                continue
            j = len(overlap) / len(target_tokens | osm_tokens)
            if j > best_score:
                best_score = j
                best = f
        if not best or best_score < 0.3:
            return {}
        return {
            "website": best.get("website"),
            "phone": best.get("phone"),
            "address": best.get("address"),
            "city": best.get("city"),
        }

    matched = []
    unmatched = []
    fallback_used = 0
    for i, r in enumerate(rows, 1):
        q = geocode_key(r)
        coords = geocode(q, cache)
        source_tag = "nominatim"
        if not coords:
            fb = local_osm_fallback(r)
            if fb:
                coords = fb
                source_tag = "osm-fallback"
                fallback_used += 1
        if i % 20 == 0:
            print(f"[geocode] {i}/{len(rows)} (cache size {len(cache)})", file=sys.stderr)
            save_cache(cache)
        if coords:
            lat, lng = coords
            enrichment = enrich_from_osm(lat, lng, r["nombre"])
            record = {
                "codigo": r["codigo"],
                "name": r["nombre"],
                "municipio": r["municipio"],
                "etapa": r["etapa"],
                "tipo": r["tipo"],
                "lat": lat,
                "lng": lng,
                "neae_types": ["TEA"],
                "ccaa": "Madrid",
                "source": "CAM 2024-25 (aulas TEA con plazas disponibles)",
                "geocoded_by": source_tag,
            }
            # добавляем контакты из OSM, если нашлись
            for k, v in enrichment.items():
                if v:
                    record[k] = v
            matched.append(record)
        else:
            unmatched.append(r)
    save_cache(cache)

    print(f"[done ] {len(matched)} geocoded "
          f"({len(matched)-fallback_used} nominatim, {fallback_used} fallback); "
          f"{len(unmatched)} unresolved", file=sys.stderr)
    for u in unmatched[:10]:
        print(f"  [no-hit] {u['nombre']} ({u['municipio']})", file=sys.stderr)

    payload = {
        "source": "Comunidad de Madrid — centros preferentes TEA 2024/25",
        "source_url": "https://www.comunidad.madrid/sites/default/files/informacion_centros_preferentes_tea_curso_2024-25.pdf",
        "description": "Школы с поддержкой NEAE: специализированные aulas TEA (аутизм). Подсписок школ с доступными местами на 2024/25.",
        "count": len(matched),
        "unmatched_count": len(unmatched),
        "features": matched,
    }
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[ok] Wrote {OUT_PATH} ({size_kb:.1f} KB, {len(matched)} schools)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
