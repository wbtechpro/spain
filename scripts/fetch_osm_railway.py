"""
Snapshot Spanish fast-rail network from OSM into a static GeoJSON file.

Output: data/osm_highspeed_rail.geojson

Two tiers:
  - "hsr":  railway=rail + (highspeed=yes | usage=high_speed) — AVE infrastructure
  - "fast": railway=rail + maxspeed >= 200 км/ч, не отмеченные как hsr
            (Alvia на иберийской колее, Euromed Barcelona-Alicante и т.п.)
"""
from __future__ import annotations
import json
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

import requests

ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = ROOT / "data" / "osm_highspeed_rail.geojson"

OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]

# Iberia + France + Benelux + север Италии — чтобы видеть продолжения
# скоростных линий в соседние страны (Париж, Лион, Лиссабон/Порту и т.д.).
# Один большой bbox упирается в таймаут Overpass, поэтому бьём на тайлы.
BBOXES = [
    "35.9,-10.0,43.9,-2.5",   # Иберия SW
    "35.9, -2.5,43.9, 4.5",   # Иберия SE + Каталония
    "43.9,-10.0,48.0,-2.5",   # Франция SW (Бордо, Тулуза)
    "43.9, -2.5,48.0, 4.5",   # Франция S+C (Лион, Марсель)
    "43.9,  4.5,48.0, 8.5",   # Франция SE + Италия север
    "48.0,-10.0,51.5,-2.5",   # Франция NW (Париж W, Бретань)
    "48.0, -2.5,51.5, 4.5",   # Франция N + Benelux (Париж, Лилль)
    "48.0,  4.5,51.5, 8.5",   # Франция NE (LGV Est, Страсбург)
]
SLEEP_BETWEEN = 10

QUERY_TEMPLATE = """
[out:json][timeout:300];
(
  way["railway"="rail"]["highspeed"="yes"]({bbox});
  way["railway"="rail"]["usage"="high_speed"]({bbox});
  way["railway"="rail"]["maxspeed"~"^(2[0-9]{{2}}|3[0-9]{{2}})$"]({bbox});
);
out geom tags;
"""


def overpass(query: str, timeout: int = 300) -> list[dict]:
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
            time.sleep(30)
    raise RuntimeError(f"all mirrors failed; last: {last_err}")


def parse_int(v: str | None) -> int | None:
    if not v:
        return None
    try:
        return int(str(v).strip())
    except ValueError:
        return None


def classify(t: dict) -> str:
    if t.get("highspeed") == "yes" or t.get("usage") == "high_speed":
        return "hsr"
    return "fast"


def to_feature(el: dict) -> dict | None:
    geom = el.get("geometry") or []
    if len(geom) < 2:
        return None
    coords = [[round(p["lon"], 6), round(p["lat"], 6)] for p in geom]
    t = el.get("tags") or {}
    return {
        "type": "Feature",
        "geometry": {"type": "LineString", "coordinates": coords},
        "properties": {
            "osm_id": el.get("id"),
            "tier": classify(t),
            "name": t.get("name") or t.get("name:es") or None,
            "ref": t.get("ref") or None,
            "operator": t.get("operator") or None,
            "maxspeed": parse_int(t.get("maxspeed")),
            "electrified": t.get("electrified") or None,
            "gauge": t.get("gauge") or None,
        },
    }


def main() -> int:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    elements: list[dict] = []
    seen_ids: set[int] = set()
    for i, bbox in enumerate(BBOXES):
        if i > 0:
            time.sleep(SLEEP_BETWEEN)
        q = QUERY_TEMPLATE.format(bbox=bbox)
        print(f"[fetch] tile {i+1}/{len(BBOXES)} bbox={bbox}", file=sys.stderr)
        try:
            els = overpass(q)
        except Exception as e:
            print(f"[error] tile {bbox}: {e}", file=sys.stderr)
            continue
        new = 0
        for el in els:
            wid = el.get("id")
            if wid in seen_ids:
                continue
            seen_ids.add(wid)
            elements.append(el)
            new += 1
        print(f"        got {len(els)} ways ({new} new)", file=sys.stderr)

    features = [f for f in (to_feature(el) for el in elements) if f]
    by_tier: dict[str, int] = {}
    for f in features:
        t = f["properties"]["tier"]
        by_tier[t] = by_tier.get(t, 0) + 1

    payload = {
        "type": "FeatureCollection",
        "source": "OpenStreetMap (via Overpass API)",
        "source_url": "https://www.openstreetmap.org/",
        "license": "ODbL 1.0 — © OpenStreetMap contributors",
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "category": "fast_rail",
        "count": len(features),
        "tiers": by_tier,
        "features": features,
    }
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[ok] {len(features)} lines → {OUT_PATH.name} ({size_kb:.0f} KB)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
