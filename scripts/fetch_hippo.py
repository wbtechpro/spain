"""
Build a curated JSON of hippotherapy / therapeutic & adaptive riding centres
in Spain. OSM tagging for this category is extremely sparse (≈3 POIs across
the country), so this script relies on hand-verified organisations whose
websites publicly describe their therapeutic programmes.

Sources per centre: official site of the organisation (verified via WebSearch
before being added here). Coordinates via Nominatim (OSM); fallback to a
city centroid when Nominatim cannot resolve the exact name.

Output: data/hippo.json
"""
from __future__ import annotations
import json
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = ROOT / "data" / "hippo.json"
CACHE_PATH = ROOT / "data" / "raw" / "hippo_geocode_cache.json"

NOMINATIM = "https://nominatim.openstreetmap.org/search"
UA = "spain-map-relocation/0.1 (curation)"

# Каждая запись: verified public org with hippotherapy / therapeutic riding.
# `query` идёт в Nominatim; если не находит — падаем в городской центроид.
# Точные координаты, подтверждённые OSM (data/osm_hippo.json) — используем их
# напрямую, минуя Nominatim.
OSM_EXACT = {
    "Equinoterapia Mas Alba":                        (42.118, 2.889),
    "Asociación Hípica Terapéutica Santa Ana":       (36.367892, -6.152784),
    "Centro de Hipoterapia Aurelio Vela":            (39.322, -3.238),
}

# Координаты конкретных объектов, которые не находятся Nominatim'ом.
# Источники — официальные страницы организаций / каталоги (Waze/esopiniones и т.п.),
# проверенные через WebSearch.
HARDCODED = {
    # Hípica El Molino (PSICAB), M-612 km 3.400, 28049 Madrid.
    # Источник: caталог esopiniones.com / cylex / Waze.
    "PSICAB — Terapia asistida con caballos":         (40.51044, -3.73360),
    # Centre Indiana — Disseminat Sant Daniel, Tordera. Координата часовни
    # Sant Daniel (41°42'10.7"N 2°45'41.8"E) — в том же районе, в ~4 км
    # от центра Тордеры; адрес "Disseminat Sant Daniel 200" относится к
    # рассеянной застройке вокруг часовни.
    "Associació Centre Indiana Equinoteràpia":        (41.70297, 2.76161),
}

CENTRES = [
    # === Madrid ===
    {"name": "ECRIN Terapias (Hípica Soto del Espinar)", "city": "Fuente el Saz de Jarama", "ccaa": "Madrid",
     "query": "Camino del Espinar, Fuente el Saz de Jarama",
     "website": "https://ecrinterapias.es/",
     "services": ["hipoterapia", "equinoterapia", "psicología con caballos"],
     "note": "На базе Hípica Soto del Espinar, ≈20 мин от Plaza de Castilla"},
    {"name": "PSICAB — Terapia asistida con caballos", "city": "Madrid", "ccaa": "Madrid",
     "query": "Hípica El Molino, Carretera M-612, Madrid",
     "website": "https://www.psicab.com/",
     "services": ["psicología asistida con caballos", "neuropsicología"],
     "note": "Hípica El Molino, M-612 km 3.400 (El Pardo / Fuencarral)"},
    {"name": "Asociación Al Paso — Terapias con caballos", "city": "Villanueva de la Cañada", "ccaa": "Madrid",
     "query": "Hípica Villafranca del Castillo",
     "website": "https://www.terapiasalpaso.org/",
     "services": ["terapia ecuestre", "equipo interdisciplinar"],
     "note": "Centro Ecuestre Villafranca del Castillo (между Majadahonda и Villanueva de la Cañada)"},
    # AEDEQ (umbrella-ассоциация) не включён: терапии не проводит,
    # физический адрес не публикуется. Ссылку можно держать в README.

    # === Catalunya ===
    {"name": "Equinoterapia Mas Alba", "city": "Sant Pere de Ribes", "ccaa": "Catalunya",
     "query": "Equinoterapia Mas Alba",
     "website": "https://www.equinoterapia.es/",
     "services": ["equinoterapia", "hipoterapia"],
     "note": "Подтверждён в OSM"},
    {"name": "Associació Centre Indiana Equinoteràpia", "city": "Tordera", "ccaa": "Catalunya",
     "query": "Disseminat Sant Daniel, Tordera",
     "website": "https://www.centreindiana.com/",
     "services": ["equinoterapia", "psicoterapia con caballos"],
     "note": "Disseminat Sant Daniel 200, Tordera, 08490"},
    {"name": "Asociación EPONA — Equinoterapia Barcelona", "city": "Cerdanyola del Vallès", "ccaa": "Catalunya",
     "query": "Campus UAB Bellaterra, Cerdanyola del Vallès",
     "website": "https://www.eponaequinoterapia.com/",
     "services": ["equinoterapia", "formación"],
     "note": "Кампус UAB, Carrer de les Granges s/n, Bellaterra"},

    # === País Vasco ===
    {"name": "Centro Ecuestre Zabolain", "city": "Arrasate-Mondragón", "ccaa": "País Vasco",
     "query": "Zabolain Arrasate",
     "website": "https://www.zabolain.com/",
     "services": ["hipoterapia", "equitación adaptada", "equinoterapia"],
     "note": "Адаптивная езда + hipoterapia"},

    # === Galicia ===
    {"name": "Asociación West Galicia — Equinoterapia", "city": "Vigo", "ccaa": "Galicia",
     "query": "Asociación West Galicia Vigo",
     "website": "https://westgalicia.es/equinoterapia/",
     "services": ["equinoterapia", "autismo", "epilepsia refractaria"],
     "note": "Точный адрес не опубликован; координата приблизительная"},

    # === Andalucía ===
    {"name": "Asociación Hispalense de Terapias Ecuestres", "city": "Sevilla", "ccaa": "Andalucía",
     "query": "Avenida Séneca, Parque Alcosa, Sevilla",
     "website": "https://hipoterapiasevilla1.wordpress.com/",
     "services": ["hipoterapia", "terapia ecuestre"],
     "note": "Parque del Tamarguillo, Av. Séneca / C. María Fulmen"},
    {"name": "EQUITEA — Centro de Equinoterapia (Finca Los Seises)", "city": "Sanlúcar la Mayor", "ccaa": "Andalucía",
     "query": "Camino Las Palmillas, Sanlúcar la Mayor",
     "website": "https://equitea.es/site/",
     "services": ["equinoterapia", "autismo", "granja escuela"],
     "note": "Finca Los Seises, Camino de las Palmillas s/n, 41800"},
    {"name": "Asociación Hípica Terapéutica Santa Ana", "city": "San Fernando", "ccaa": "Andalucía",
     "query": "Hípica Terapéutica Santa Ana, Cádiz",
     "website": None,
     "services": ["hipoterapia", "terapia ecuestre"],
     "note": "Кадис; подтверждён в OSM"},

    # === Castilla-La Mancha ===
    {"name": "Centro de Hipoterapia Aurelio Vela", "city": "Alcázar de San Juan", "ccaa": "Castilla-La Mancha",
     "query": "Centro de Hipoterapia Aurelio Vela",
     "website": "https://policlinicaaureliovela.es/terapias-asistidas-con-caballos/",
     "services": ["hipoterapia", "terapias asistidas con caballos"],
     "note": "Ciudad Real; подтверждён в OSM"},

    # === Comunidad Valenciana ===
    {"name": "El Mas de Xetà — Terapias con caballos", "city": "Llutxent", "ccaa": "Valenciana",
     "query": "Camí de Xetà, Llutxent",
     "website": "https://elmasdexeta.com/terapias/",
     "services": ["terapia con caballos", "diversidad funcional"],
     "note": "Camí de Xetà s/n, 46838 Llutxent, Vall d'Albaida"},
]


CITY_CENTROIDS = {
    "Madrid":                 (40.4168, -3.7038),
    "Fuente el Saz de Jarama":(40.6260, -3.5080),
    "Villanueva de la Cañada":(40.4407, -4.0056),
    "Barcelona":              (41.3851,  2.1734),
    "Sant Pere de Ribes":     (41.2630,  1.7720),
    "Tordera":                (41.6996,  2.7199),
    "Cerdanyola del Vallès":  (41.4911,  2.1410),
    "Arrasate-Mondragón":     (43.0664, -2.4897),
    "Vigo":                   (42.2406, -8.7207),
    "Sevilla":                (37.3891, -5.9845),
    "Sanlúcar la Mayor":      (37.3866, -6.2028),
    "San Fernando":           (36.4631, -6.1947),
    "Alcázar de San Juan":    (39.3906, -3.2062),
    "Llutxent":               (38.9167, -0.3478),
}


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


def main():
    cache = load_cache()
    features = []
    # Счётчик повторов координаты для jitter, чтобы маркеры не ложились стопкой.
    used = {}
    for c in CENTRES:
        osm_name = None
        if c["name"] in OSM_EXACT:
            lat, lng = OSM_EXACT[c["name"]]
            precision = "exact (OSM)"
        elif c["name"] in HARDCODED:
            lat, lng = HARDCODED[c["name"]]
            precision = "exact (hardcoded)"
        else:
            hit = geocode(c["query"], cache)
            if hit:
                lat, lng, osm_name = hit
                precision = "exact (nominatim)"
            else:
                centroid = CITY_CENTROIDS.get(c["city"])
                if not centroid:
                    print(f"[skip] no coords for {c['name']}", file=sys.stderr)
                    continue
                lat, lng = centroid
                precision = "approx (city centroid)"
        # Jitter для повторяющихся точек (разные центры в одном городе).
        key = (round(lat, 3), round(lng, 3))
        n = used.get(key, 0)
        if n:
            # Смещение ~50-150 м по кругу, шаг ~60°
            import math
            r = 0.0008 + 0.0004 * (n // 6)
            a = math.radians(60 * n)
            lat += r * math.cos(a)
            lng += r * math.sin(a)
            precision += f" (+jitter#{n})"
        used[key] = n + 1
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
        "source": "Ручная курация: AEDEQ + публичные сайты центров, верифицированных по веб-поиску",
        "period": "2026-04",
        "metric": "иппотерапия, лечебная и адаптивная верховая езда",
        "count": len(features),
        "license": "Публичные данные с официальных сайтов учреждений",
        "features": features,
    }
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[ok] Wrote {OUT_PATH} ({size_kb:.1f} KB, {len(features)} centres)", file=sys.stderr)


if __name__ == "__main__":
    main()
