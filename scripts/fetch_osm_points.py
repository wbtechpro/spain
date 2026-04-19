"""
Snapshot all OSM point categories for Spain into static JSON files.
Eliminates the need for live Overpass queries in the browser.

Output: data/osm_{hospitals,schools,spanish,yoga,hippo}.json

Run periodically (monthly?) to refresh. Overpass public instances rate-limit,
so we use kumi.systems as primary with overpass-api.de as fallback.
"""
from __future__ import annotations
import json
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

import requests

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data"

OVERPASS_URLS = [
    # osm.ch возвращает HTTP 200 с пустым ответом (timestamp вида "113573"
    # вместо ISO-даты) — по сути битое зеркало, пропускаем.
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]

# Spain split into smaller bboxes so each query fits under Overpass slot budget.
# Queries on full country bbox for dense categories (schools) kept getting 504.
BBOXES = [
    "40.0,-9.5,43.9,-2.5",    # NW mainland
    "40.0,-2.5,43.9, 4.5",    # NE mainland (incl. Balearics)
    "35.9,-9.5,40.0,-2.5",    # SW mainland
    "35.9,-2.5,40.0, 4.5",    # SE mainland
    "27.5,-18.5,29.5,-13.0",  # Canaries
]
SLEEP_BETWEEN = 15   # seconds between consecutive Overpass calls
RETRY_SLEEP   = 45   # seconds to wait before retrying a failed tile

# Overpass query templates per category. Each {bbox} will be substituted.
QUERIES = {
    "hospitals": """
        node["amenity"="hospital"]({bbox});
        way["amenity"="hospital"]({bbox});
    """,
    "schools": """
        node["amenity"="school"]({bbox});
        way["amenity"="school"]({bbox});
    """,
    "spanish": """
        node["amenity"="language_school"]({bbox});
        way["amenity"="language_school"]({bbox});
    """,
    "yoga": """
        node["sport"="yoga"]({bbox});
        node["leisure"="fitness_centre"]["name"~"yoga",i]({bbox});
        node["shop"="yoga"]({bbox});
        node["amenity"="yoga"]({bbox});
        way["sport"="yoga"]({bbox});
    """,
    # Иппотерапия / лечебная и адаптивная верховая езда.
    # В OSM чистого тега нет, а конные клубы почти не помечают
    # терапевтическую функцию. Полнотекстовый поиск по name/description/
    # operator даёт лишь горсть совпадений — этот слой лучше дополнять
    # курируемым списком (см. data/neurorehab.json как образец).
    "hippo": """
        nwr["healthcare"="hippotherapy"]({bbox});
        nwr["name"~"hipoterap|equinoterap",i]({bbox});
        nwr["description"~"hipoterap|equinoterap",i]({bbox});
        nwr["operator"~"hipoterap|equinoterap",i]({bbox});
        nwr["leisure"="horse_riding"]["name"~"terap|adaptad",i]({bbox});
        nwr["sport"="equestrian"]["name"~"terap|adaptad",i]({bbox});
    """,
    "slp": """
        node["healthcare"="speech_therapist"]({bbox});
        way["healthcare"="speech_therapist"]({bbox});
        node["healthcare:speciality"="speech_therapy"]({bbox});
        way["healthcare:speciality"="speech_therapy"]({bbox});
        node["healthcare:speciality"="speech-language_pathology"]({bbox});
        node["practice"="speech_therapy"]({bbox});
        node["amenity"="clinic"]["name"~"logoped",i]({bbox});
        node["office"~"therapist",i]["name"~"logoped",i]({bbox});
        node["healthcare"~"clinic|centre",i]["name"~"logoped",i]({bbox});
    """,
}


def overpass(query: str, timeout: int = 240) -> list[dict]:
    """Run Overpass query, retrying across mirrors with exponential backoff."""
    body = "data=" + query
    last_err = None
    for attempt in range(3):
        for url in OVERPASS_URLS:
            try:
                r = requests.post(
                    url,
                    data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=timeout,
                )
                if r.status_code == 200:
                    return r.json().get("elements", []) or []
                last_err = f"HTTP {r.status_code} from {url}"
                print(f"[warn] attempt {attempt+1}: {last_err}", file=sys.stderr)
            except Exception as e:
                last_err = f"{type(e).__name__}: {e} from {url}"
                print(f"[warn] attempt {attempt+1}: {last_err}", file=sys.stderr)
            time.sleep(3)
        if attempt < 2:
            print(f"[wait] backing off {RETRY_SLEEP}s before retry…", file=sys.stderr)
            time.sleep(RETRY_SLEEP)
    raise RuntimeError(f"all mirrors failed; last: {last_err}")


def fetch_category(cat: str) -> list[dict]:
    """Fetch across all tile bboxes, sequentially with pauses."""
    frags = QUERIES[cat].strip()
    elements = []
    for i, bbox in enumerate(BBOXES):
        q = f"[out:json][timeout:240];({frags.format(bbox=bbox)});out center;"
        print(f"[fetch] {cat} bbox={bbox}", file=sys.stderr)
        try:
            els = overpass(q)
            print(f"        got {len(els)} elements", file=sys.stderr)
            elements.extend(els)
        except Exception as e:
            print(f"[error] {cat} bbox={bbox}: {e}", file=sys.stderr)
        if i < len(BBOXES) - 1:
            time.sleep(SLEEP_BETWEEN)
    return elements


def to_feature(el: dict) -> dict | None:
    """Normalize OSM element into our compact feature format."""
    lat = el.get("lat") or (el.get("center") or {}).get("lat")
    lon = el.get("lon") or (el.get("center") or {}).get("lon")
    if lat is None or lon is None:
        return None
    t = el.get("tags") or {}
    name = t.get("name") or t.get("name:ru") or t.get("name:en")
    if not name:
        return None
    addr_parts = [t.get("addr:street"), t.get("addr:housenumber")]
    addr = " ".join(p for p in addr_parts if p) or None
    return {
        "name": name,
        "lat": round(float(lat), 6),
        "lng": round(float(lon), 6),
        "city":     t.get("addr:city") or None,
        "address":  addr,
        "website":  t.get("website") or t.get("contact:website") or None,
        "phone":    t.get("phone") or t.get("contact:phone") or None,
        "hours":    t.get("opening_hours") or None,
        "kind":     t.get("healthcare") or t.get("school:type") or t.get("sport") or None,
    }


def dedup(features: list[dict]) -> list[dict]:
    """Drop duplicates that share name+coords rounded to ~100m."""
    seen = set()
    out = []
    for f in features:
        key = (f["name"].lower().strip(), round(f["lat"], 3), round(f["lng"], 3))
        if key in seen:
            continue
        seen.add(key)
        out.append(f)
    return out


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    only = sys.argv[1:] or list(QUERIES.keys())
    for i, cat in enumerate(only):
        if i > 0:
            print(f"[wait] pausing {SLEEP_BETWEEN}s between categories…", file=sys.stderr)
            time.sleep(SLEEP_BETWEEN)
        if cat not in QUERIES:
            print(f"[skip] unknown category: {cat}", file=sys.stderr)
            continue
        try:
            elements = fetch_category(cat)
        except Exception as e:
            print(f"[error] {cat}: {e}", file=sys.stderr)
            continue
        features = [f for f in (to_feature(el) for el in elements) if f]
        features = dedup(features)
        payload = {
            "source": "OpenStreetMap (via Overpass API)",
            "source_url": "https://www.openstreetmap.org/",
            "license": "ODbL 1.0 — © OpenStreetMap contributors",
            "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "category": cat,
            "count": len(features),
            "features": features,
        }
        out_path = OUT_DIR / f"osm_{cat}.json"
        out_path.write_text(json.dumps(payload, ensure_ascii=False))
        size_kb = out_path.stat().st_size / 1024
        print(f"[ok] {cat}: {len(features)} features → {out_path.name} ({size_kb:.0f} KB)",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
