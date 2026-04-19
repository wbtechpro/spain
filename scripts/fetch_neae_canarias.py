"""
Parse Canarias 'Aulas Enclave' HTML pages — schools with special education classrooms.

Source: https://www.gobiernodecanarias.org/educacion/web/centros/docentes-y-no-docentes/publicos/docentes/centros-aulasenclave/
Four sub-pages (one per island group) with HTML tables:
  CENTRO | MUNICIPIO | Nº UD. | EOEP

Output: merged into data/schools_neae.json with ccaa='Canarias'.
"""
from __future__ import annotations
import json
import re
import shutil
import sys
import time
import unicodedata
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = ROOT / "data" / "schools_neae.json"
CACHE_PATH = ROOT / "data" / "raw" / "canarias_geocode_cache.json"
HTML_DIR = ROOT / "data" / "raw"

SOURCES = {
    "Gran Canaria":          ("can_gc.html",  "https://www.gobiernodecanarias.org/educacion/web/centros/docentes-y-no-docentes/publicos/docentes/centros-aulasenclave/aulas-enclave-gran-canaria/index.html"),
    "Tenerife":              ("can_tf.html",  "https://www.gobiernodecanarias.org/educacion/web/centros/docentes-y-no-docentes/publicos/docentes/centros-aulasenclave/aulas-enclave-de-tenerife/index.html"),
    "Lanzarote/Fuerteventura": ("can_lf.html", "https://www.gobiernodecanarias.org/educacion/web/centros/docentes-y-no-docentes/publicos/docentes/centros-aulasenclave/aulas-enclave-de-lanzarote-y-fuerteventura/index.html"),
    "Gomera/Palma/Hierro":   ("can_gph.html", "https://www.gobiernodecanarias.org/educacion/web/centros/docentes-y-no-docentes/publicos/docentes/centros-aulasenclave/aulas-enclave-gomera-palma-hierro/index.html"),
}

NOMINATIM = "https://nominatim.openstreetmap.org/search"
UA = "spain-map-relocation/0.1 (curation)"

# Canarias bbox
CANARIAS_BBOX = (27.5, -18.5, 29.5, -13.0)


def download_html():
    for island, (fname, url) in SOURCES.items():
        dest = HTML_DIR / fname
        if dest.exists() and dest.stat().st_size > 10000:
            continue
        tmp = Path(f"/tmp/{fname}")
        if tmp.exists():
            shutil.copy(tmp, dest)
            continue
        r = requests.get(url,
                         headers={"User-Agent": "Mozilla/5.0 Chrome/122", "Accept-Language": "es"},
                         timeout=30)
        r.raise_for_status()
        dest.write_bytes(r.content)


def parse_pages() -> list[dict]:
    out = []
    for island, (fname, _url) in SOURCES.items():
        path = HTML_DIR / fname
        if not path.exists():
            # fallback to /tmp
            path = Path(f"/tmp/{fname}")
        soup = BeautifulSoup(path.read_text(), "lxml")
        for tbl in soup.find_all("table"):
            rows = tbl.find_all("tr")
            if not rows:
                continue
            # header should have CENTRO, MUNICIPIO
            header = [td.get_text(" ", strip=True) for td in rows[0].find_all(["th", "td"])]
            if not any("CENTRO" in h.upper() for h in header):
                continue
            for tr in rows[1:]:
                cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
                if len(cells) < 2:
                    continue
                centro, municipio = cells[0], cells[1]
                if not centro:
                    continue
                # Split type prefix (CEIP, IES, CEO, etc.) from name
                m = re.match(r"^(CEIP|CEIPS|IES|IESO|CEO|CER|CEE|CC|EEI|EOEP)\s+(.*)$", centro)
                if m:
                    tipo, name = m.group(1), m.group(2)
                else:
                    tipo, name = "", centro
                out.append({
                    "tipo": tipo,
                    "name_raw": name,
                    "municipio": municipio,
                    "island": island,
                })
    return out


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
        if not (CANARIAS_BBOX[0] <= lat <= CANARIAS_BBOX[2]
                and CANARIAS_BBOX[1] <= lng <= CANARIAS_BBOX[3]):
            cache[q] = None
            return None
        cache[q] = {"lat": lat, "lng": lng}
        return lat, lng
    except Exception as e:
        print(f"[warn] {q[:60]}: {e}", file=sys.stderr)
        return None


def main() -> int:
    download_html()
    rows = parse_pages()
    print(f"[parse] {len(rows)} Canarias schools", file=sys.stderr)

    cache = load_cache()
    features = []
    unmatched = []
    for r in rows:
        queries = [
            f"{r['tipo']} {r['name_raw']}, {r['municipio']}, Canarias, Spain",
            f"{r['name_raw']}, {r['municipio']}, Canarias, Spain",
            f"{r['name_raw']}, {r['municipio']}, Spain",
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
            "name": r["name_raw"],
            "municipio": r["municipio"],
            "island": r["island"],
            "etapa": "INFANTIL-PRIMARIA-SECUNDARIA",
            "tipo": r["tipo"],
            "lat": lat, "lng": lng,
            "neae_types": ["Aula Enclave"],
            "ccaa": "Canarias",
            "source": "Gobierno de Canarias — Aulas Enclave (HTML tables)",
        })
        if len(features) % 25 == 0:
            save_cache(cache)
            print(f"[geo] {len(features)}/{len(rows)}", file=sys.stderr)
    save_cache(cache)
    print(f"[done] {len(features)} Canarias geocoded; {len(unmatched)} unresolved",
          file=sys.stderr)

    existing = json.loads(OUT_PATH.read_text()) if OUT_PATH.exists() else {"features": []}
    keep = [f for f in existing.get("features", []) if f.get("ccaa") != "Canarias"]
    merged = keep + features
    payload = {
        "source": "CAM + DOGV Valencia + Andalucía + Murcia + Castilla-La Mancha + Canarias",
        "count": len(merged),
        "breakdown_by_ccaa": {
            ccaa: sum(1 for f in merged if f.get("ccaa") == ccaa)
            for ccaa in ("Madrid", "Valenciana", "Andalucía", "Murcia",
                         "Castilla-La Mancha", "Canarias")
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
