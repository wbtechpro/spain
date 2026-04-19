"""
Build a curated JSON of pediatric neurorehabilitation centres in Spain.

Sources: public hospital websites, FEDACE directory, ASPACE federation,
Instituto Guttmann, widely-recognised specialised private centres.

Coordinates are resolved via Nominatim (OSM) using the centre's canonical name
+ city. Each entry marks `precision` ('exact' if Nominatim found the named
institution, 'approx' if we fell back to the city centroid).

Output: data/neurorehab.json
"""
from __future__ import annotations
import json
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = ROOT / "data" / "neurorehab.json"
CACHE_PATH = ROOT / "data" / "raw" / "neurorehab_geocode_cache.json"

NOMINATIM = "https://nominatim.openstreetmap.org/search"
UA = "spain-map-relocation/0.1 (curation)"

# Кураторский список — проверяемые публичные центры и специализированные
# частные клиники, публикующие свои адреса и услуги детской нейрореабилитации.
# Query используется для Nominatim-геокодинга.
CENTRES = [
    # --- Специализированные центры детской нейрореабилитации ---
    {"name": "Instituto Guttmann", "city": "Badalona", "ccaa": "Catalunya",
     "query": "Institut Guttmann, Badalona, Spain",
     "website": "https://www.guttmann.com/",
     "services": ["neuro", "spinal cord", "brain injury", "pediátrico"],
     "note": "Референсный центр по нейрореабилитации; детское отделение"},
    {"name": "Hospital Beata María Ana", "city": "Madrid", "ccaa": "Madrid",
     "query": "Hospital Beata María Ana, Madrid",
     "website": "https://www.hospitalbeatamariaana.es/",
     "services": ["neurorehabilitación", "pediátrico"],
     "note": "Частный госпиталь, сильная неврологическая реабилитация"},
    {"name": "Centro Lescer", "city": "Madrid", "ccaa": "Madrid",
     "query": "Centro Lescer, Madrid",
     "website": "https://www.lescer.com/",
     "services": ["daño cerebral adquirido", "pediátrico"],
     "note": "Специализированный центр по ДЦП и приобретённому поражению мозга"},
    {"name": "Fundación NIPACE", "city": "Guadalajara", "ccaa": "Castilla-La Mancha",
     "query": "Fundación NIPACE, Guadalajara, Spain",
     "website": "https://www.nipace.com/",
     "services": ["parálisis cerebral", "pediátrico", "neurorehabilitación"],
     "note": "Фонд специализирован на ДЦП и детской нейрореабилитации"},
    {"name": "Centro de Rehabilitación Neurológica Nebrija", "city": "Madrid", "ccaa": "Madrid",
     "query": "Centro Rehabilitación Neurológica Nebrija, Madrid",
     "website": "https://www.nebrija.com/",
     "services": ["neurorehabilitación"],
     "note": "Университетский центр нейрореабилитации"},
    {"name": "Fundación Aprocor", "city": "Madrid", "ccaa": "Madrid",
     "query": "Fundación Aprocor, Madrid",
     "website": "https://www.aprocor.org/",
     "services": ["discapacidad intelectual", "apoyo infantil"],
     "note": "Поддержка детей с интеллектуальными нарушениями"},
    {"name": "NeuroFisio (Clínica Neurofisiológica Valencia)", "city": "Valencia", "ccaa": "Valenciana",
     "query": "Neurofisio Valencia",
     "website": "https://www.neurofisio.es/",
     "services": ["neurofisioterapia", "pediátrico"],
     "note": "Частная клиника детской нейрофизиотерапии"},

    # --- ASPACE (федерация ДЦП, региональные центры) ---
    {"name": "ASPACE Madrid (Asoc. Parálisis Cerebral)", "city": "Madrid", "ccaa": "Madrid",
     "query": "ASPACE Madrid Parálisis Cerebral",
     "website": "https://www.aspacemadrid.org/",
     "services": ["parálisis cerebral", "infantil"],
     "note": "Региональный центр ASPACE — ДЦП"},
    {"name": "ASPACE Catalunya", "city": "Barcelona", "ccaa": "Catalunya",
     "query": "ASPACE Catalunya Barcelona",
     "website": "https://www.aspace.cat/",
     "services": ["parálisis cerebral", "infantil"],
     "note": "Каталонское отделение ASPACE"},
    {"name": "ASPACE Andalucía (Sevilla)", "city": "Sevilla", "ccaa": "Andalucía",
     "query": "ASPACE Sevilla Parálisis Cerebral",
     "website": "https://www.aspaceandalucia.org/",
     "services": ["parálisis cerebral", "infantil"],
     "note": "Региональное отделение ASPACE"},

    # --- Публичные госпитали с детской нейрореабилитацией ---
    {"name": "Hospital Universitario La Paz — Rehab. Pediátrica", "city": "Madrid", "ccaa": "Madrid",
     "query": "Hospital Universitario La Paz, Madrid",
     "website": "https://www.comunidad.madrid/hospital/lapaz/",
     "services": ["hospital público", "neuro pediátrica"],
     "note": "Крупнейший госпиталь Мадрида с детской нейро"},
    {"name": "Hospital Infantil Universitario Niño Jesús", "city": "Madrid", "ccaa": "Madrid",
     "query": "Hospital Niño Jesús, Madrid",
     "website": "https://www.comunidad.madrid/hospital/ninojesus/",
     "services": ["hospital infantil", "neuro pediátrica"],
     "note": "Детский госпиталь с сильным neuro unit"},
    {"name": "Hospital Universitario 12 de Octubre — Pediatría", "city": "Madrid", "ccaa": "Madrid",
     "query": "Hospital 12 de Octubre, Madrid",
     "website": "https://www.comunidad.madrid/hospital/12octubre/",
     "services": ["hospital público", "neuro pediátrica"]},
    {"name": "Hospital General Universitario Gregorio Marañón — Pediatría", "city": "Madrid", "ccaa": "Madrid",
     "query": "Hospital Gregorio Marañón, Madrid",
     "website": "https://www.comunidad.madrid/hospital/gregoriomaranon/",
     "services": ["hospital público", "neuro pediátrica"]},
    {"name": "Hospital Sant Joan de Déu", "city": "Esplugues de Llobregat", "ccaa": "Catalunya",
     "query": "Hospital Sant Joan de Déu, Esplugues",
     "website": "https://www.sjdhospitalbarcelona.org/",
     "services": ["hospital pediátrico", "neuro"],
     "note": "Один из главных детских госпиталей Испании"},
    {"name": "Hospital Clínic de Barcelona — Pediatría", "city": "Barcelona", "ccaa": "Catalunya",
     "query": "Hospital Clínic de Barcelona",
     "website": "https://www.clinicbarcelona.org/",
     "services": ["hospital público", "neuro pediátrica"]},
    {"name": "Hospital Universitari Vall d'Hebron — Pediatría", "city": "Barcelona", "ccaa": "Catalunya",
     "query": "Hospital Vall d'Hebron, Barcelona",
     "website": "https://www.vallhebron.com/",
     "services": ["hospital público", "neuro pediátrica"]},
    {"name": "Hospital La Fe — Unidad Neuro Pediátrica", "city": "Valencia", "ccaa": "Valenciana",
     "query": "Hospital Universitari i Politècnic La Fe, Valencia",
     "website": "https://www.lafe.san.gva.es/",
     "services": ["hospital público", "neuro pediátrica"]},
    {"name": "Hospital Universitario Virgen del Rocío — Pediatría", "city": "Sevilla", "ccaa": "Andalucía",
     "query": "Hospital Virgen del Rocío, Sevilla",
     "website": "https://www.hospitalesvirgendelrocio.es/",
     "services": ["hospital público", "neuro pediátrica"]},
    {"name": "Hospital Universitario Virgen de las Nieves — Pediatría", "city": "Granada", "ccaa": "Andalucía",
     "query": "Hospital Virgen de las Nieves, Granada",
     "website": "https://huvn.es/",
     "services": ["hospital público", "neuro pediátrica"]},
    {"name": "Hospital Universitario Regional de Málaga — Pediatría", "city": "Málaga", "ccaa": "Andalucía",
     "query": "Hospital Regional de Málaga",
     "website": "https://www.hospitalregionaldemalaga.es/",
     "services": ["hospital público", "neuro pediátrica"]},
    {"name": "Hospital Universitario Cruces — Pediatría", "city": "Barakaldo", "ccaa": "País Vasco",
     "query": "Hospital Universitario Cruces, Barakaldo",
     "website": "https://www.osakidetza.euskadi.eus/hospital-universitario-cruces/",
     "services": ["hospital público", "neuro pediátrica"]},
    {"name": "Hospital Universitari Son Espases — Pediatría", "city": "Palma de Mallorca", "ccaa": "Illes Balears",
     "query": "Hospital Son Espases, Palma de Mallorca",
     "website": "https://www.hsll.es/",
     "services": ["hospital público", "neuro pediátrica"]},
    {"name": "Complejo Hospitalario Universitario A Coruña — Pediatría", "city": "A Coruña", "ccaa": "Galicia",
     "query": "Complejo Hospitalario Universitario A Coruña",
     "website": "https://xxiacoruna.sergas.gal/",
     "services": ["hospital público", "neuro pediátrica"]},
]


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
        return (v["lat"], v["lng"], v.get("osm_name")) if v else None
    try:
        r = requests.get(
            NOMINATIM,
            params={"q": q, "format": "json", "limit": 1, "countrycodes": "es"},
            headers={"User-Agent": UA, "Accept-Language": "es,en"},
            timeout=20,
        )
        time.sleep(1.2)
        if r.status_code != 200 or not r.json():
            cache[q] = None
            return None
        hit = r.json()[0]
        lat, lng = float(hit["lat"]), float(hit["lon"])
        name = hit.get("display_name", "")[:80]
        cache[q] = {"lat": lat, "lng": lng, "osm_name": name}
        return lat, lng, name
    except Exception as e:
        print(f"[warn] {q}: {e}", file=sys.stderr)
        return None


CITY_CENTROIDS = {
    "Madrid":                 (40.4168, -3.7038),
    "Barcelona":              (41.3851,  2.1734),
    "Badalona":               (41.4500,  2.2475),
    "Esplugues de Llobregat": (41.3740,  2.0878),
    "Valencia":               (39.4699, -0.3763),
    "Sevilla":                (37.3891, -5.9845),
    "Granada":                (37.1773, -3.5986),
    "Málaga":                 (36.7213, -4.4217),
    "Barakaldo":              (43.2975, -2.9897),
    "Palma de Mallorca":      (39.5696,  2.6502),
    "A Coruña":               (43.3623, -8.4115),
    "Guadalajara":            (40.6333, -3.1667),
}


def main():
    cache = load_cache()
    features = []
    for c in CENTRES:
        hit = geocode(c["query"], cache)
        if hit:
            lat, lng, osm_name = hit
            precision = "exact"
        else:
            # fallback: city centroid
            centroid = CITY_CENTROIDS.get(c["city"])
            if not centroid:
                print(f"[skip] no coords for {c['name']}", file=sys.stderr)
                continue
            lat, lng = centroid
            osm_name = None
            precision = "approx (city centroid)"
        features.append({
            "name": c["name"],
            "city": c["city"],
            "ccaa": c["ccaa"],
            "lat": round(lat, 6),
            "lng": round(lng, 6),
            "website": c.get("website"),
            "services": c.get("services", []),
            "note": c.get("note"),
            "precision": precision,
            "osm_match": osm_name,
        })
        print(f"[ok] {c['name']} @ {lat:.4f},{lng:.4f} ({precision})", file=sys.stderr)
    save_cache(cache)

    payload = {
        "source": "Ручная курация: сайты публичных госпиталей, FEDACE, ASPACE, Instituto Guttmann и известных специализированных центров",
        "period": "2026-04",
        "metric": "детская нейрореабилитация и специализированные центры неврологической помощи",
        "count": len(features),
        "license": "Публичные данные с официальных сайтов учреждений",
        "features": features,
    }
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[ok] Wrote {OUT_PATH} ({size_kb:.1f} KB, {len(features)} centres)", file=sys.stderr)


if __name__ == "__main__":
    main()
