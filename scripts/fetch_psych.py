"""
Build a curated JSON of child/adolescent psychologists, neuropsychologists
and psychiatrists in Spain — focused on kids with neurodevelopmental
profiles (CAS, TEA, TDAH, comorbid anxiety from communication issues).

Two slices in one file:
  - kind=usmij  → public Unidades de Salud Mental Infanto-Juvenil
                  (free via SNS, every CCAA has them attached to major hospitals).
  - kind=psicologo|neuropsicologo|psiquiatra
                → curated private specialists with explicit language tags;
                  priority to russian-speaking.

Coordinates are pre-resolved per entry so this script is offline. If you
add a new entry without lat/lng, run it with network — it falls back to
Nominatim and caches the result.

Output: data/psych.json (same shape as data/speech_ru.json).
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = ROOT / "data" / "psych.json"
CACHE_PATH = ROOT / "data" / "raw" / "psych_geocode_cache.json"

NOMINATIM = "https://nominatim.openstreetmap.org/search"
UA = "spain-map-relocation/0.1 (curation)"

# Each entry mirrors data/speech_ru.json schema, with psych-specific fields:
#   kind:     "psicologo" | "neuropsicologo" | "psiquiatra" | "usmij"
#   focus:    list from {cas, tea, tdah, ansiedad, general}
#   public:   true (SNS / free) | false (private)
#   languages: list from {ru, uk, es, ca, en, de, fr, ...}
#   ages:     "child" | "adolescent" | "both"
#   priority: bool — highlight on map (top recommendation)
ENTRIES: list[dict] = [
    # ============================================================
    # PUBLIC — USMIJ at major SNS hospitals (verified institutions)
    # Free through Sistema Nacional de Salud, referral required
    # via primary-care pediatrician.
    # ============================================================
    {
        "name": "USMIJ — Hospital Infantil Universitario Niño Jesús",
        "city": "Madrid", "ccaa": "Madrid",
        "lat": 40.4117, "lng": -3.6730,
        "website": "https://www.comunidad.madrid/hospital/ninojesus/",
        "kind": "usmij",
        "focus": ["general", "tea", "tdah", "ansiedad"],
        "languages": ["es"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "note": "Референсный детский госпиталь Мадрида. Психиатрия и психология для детей/подростков.",
        "precision": "exact", "verified": True, "priority": True,
    },
    {
        "name": "Hospital Sant Joan de Déu — Salud Mental Infanto-Juvenil",
        "city": "Esplugues de Llobregat", "ccaa": "Catalunya",
        "lat": 41.3740, "lng": 2.0878,
        "website": "https://www.sjdhospitalbarcelona.org/es/salud-mental",
        "kind": "usmij",
        "focus": ["general", "tea", "tdah", "ansiedad"],
        "languages": ["es", "ca"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "note": "Один из ведущих европейских детских госпиталей. Сильная психиатрия + neuropsicología.",
        "precision": "exact", "verified": True, "priority": True,
    },
    {
        "name": "Hospital Vall d'Hebron — Psiquiatría Infanto-Juvenil",
        "city": "Barcelona", "ccaa": "Catalunya",
        "lat": 41.4271, "lng": 2.1490,
        "website": "https://www.vallhebron.com/es/asistencia/cartera-de-servicios/psiquiatria-infantil-y-juvenil",
        "kind": "usmij",
        "focus": ["general", "tea", "tdah"],
        "languages": ["es", "ca"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "note": "Государственная университетская клиника, отделение детской психиатрии.",
        "precision": "exact", "verified": True,
    },
    {
        "name": "Hospital Universitario La Paz — Psiquiatría Infanto-Juvenil",
        "city": "Madrid", "ccaa": "Madrid",
        "lat": 40.4796, "lng": -3.6913,
        "website": "https://www.comunidad.madrid/hospital/lapaz/",
        "kind": "usmij",
        "focus": ["general", "tea", "tdah"],
        "languages": ["es"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "Hospital Gregorio Marañón — Psiquiatría del Niño y Adolescente",
        "city": "Madrid", "ccaa": "Madrid",
        "lat": 40.4205, "lng": -3.6675,
        "website": "https://www.comunidad.madrid/hospital/gregoriomaranon/",
        "kind": "usmij",
        "focus": ["general", "tea", "tdah"],
        "languages": ["es"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "Hospital La Fe — Psiquiatría Infanto-Juvenil",
        "city": "Valencia", "ccaa": "Valenciana",
        "lat": 39.4427, "lng": -0.3784,
        "website": "https://www.lafe.san.gva.es/",
        "kind": "usmij",
        "focus": ["general", "tea", "tdah"],
        "languages": ["es"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "Hospital Virgen del Rocío — USMIJ",
        "city": "Sevilla", "ccaa": "Andalucía",
        "lat": 37.3580, "lng": -5.9846,
        "website": "https://www.hospitalesvirgendelrocio.es/",
        "kind": "usmij",
        "focus": ["general", "tea", "tdah"],
        "languages": ["es"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "Hospital Regional de Málaga — USMIJ",
        "city": "Málaga", "ccaa": "Andalucía",
        "lat": 36.7155, "lng": -4.4470,
        "website": "https://www.hospitalregionaldemalaga.es/",
        "kind": "usmij",
        "focus": ["general", "tea", "tdah"],
        "languages": ["es"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "Hospital Universitario Cruces — Psiquiatría Infanto-Juvenil",
        "city": "Barakaldo", "ccaa": "País Vasco",
        "lat": 43.2891, "lng": -2.9939,
        "website": "https://www.osakidetza.euskadi.eus/hospital-universitario-cruces/",
        "kind": "usmij",
        "focus": ["general", "tea", "tdah"],
        "languages": ["es", "eu"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "Hospital Son Espases — USMIJ",
        "city": "Palma de Mallorca", "ccaa": "Illes Balears",
        "lat": 39.5959, "lng": 2.6359,
        "website": "https://www.hsll.es/",
        "kind": "usmij",
        "focus": ["general", "tea", "tdah"],
        "languages": ["es", "ca"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },

    # ============================================================
    # PRIVATE — multilingual / russian-speaking specialists
    # Курируем только тех, у кого язык/специализация подтверждены
    # на собственном сайте. Где не уверен — verified: false,
    # требует ручной проверки.
    # ============================================================
    {
        "name": "Sinews Multilingual Therapy Institute",
        "city": "Madrid", "ccaa": "Madrid",
        "lat": 40.4266, "lng": -3.6992,
        "website": "https://www.sinews.es/en/child-psychology/",
        "kind": "psicologo",
        "focus": ["general", "ansiedad", "tdah"],
        "languages": ["es", "en", "de", "fr", "pl", "it", "ro", "nl", "zh", "ar", "sk"],
        "public": False, "online": True, "offline": True,
        "ages": "both",
        "note": "Sagasta 16. Мультиязычный центр, русского нет, но широкий спектр языков. Также есть neuropsicología.",
        "precision": "approx", "verified": False,
    },
    {
        "name": "Hellerhofkidz — русскоязычный детский центр",
        "city": "Marbella", "ccaa": "Andalucía",
        "lat": 36.4985, "lng": -4.9527,
        "website": "https://www.hellerhofkidz.com/",
        "kind": "psicologo",
        "focus": ["general", "ansiedad"],
        "languages": ["ru", "uk", "en", "es"],
        "public": False, "online": True, "offline": True,
        "ages": "both",
        "note": "Nueva Andalucía. Лицензия Andalucía N°13359. Психология + логопедия в одном центре.",
        "precision": "approx", "verified": False, "priority": True,
    },
    {
        "name": "Centro Bienestar Emocional — psicólogo infantil ruso (Barcelona)",
        "city": "Barcelona", "ccaa": "Catalunya",
        "lat": 41.3879, "lng": 2.1700,
        "website": "https://russianspain.com/",
        "kind": "psicologo",
        "focus": ["general", "ansiedad"],
        "languages": ["ru", "es"],
        "public": False, "online": True, "offline": True,
        "ages": "both",
        "note": "Плейсхолдер: ищется русскоязычный детский психолог в BCN через каталог russianspain.com — заменить на конкретное имя после ручной проверки.",
        "precision": "approx", "verified": False,
    },
]


def load_cache():
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text())
    return {}


def save_cache(cache):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=1))


def geocode(query, cache):
    """Lazy import requests so the script runs offline if all entries have lat/lng."""
    if query in cache:
        v = cache[query]
        return (v["lat"], v["lng"]) if v else None
    import requests
    try:
        r = requests.get(
            NOMINATIM,
            params={"q": query, "format": "json", "limit": 1, "countrycodes": "es"},
            headers={"User-Agent": UA, "Accept-Language": "es,en"},
            timeout=20,
        )
        time.sleep(1.2)
        if r.status_code != 200 or not r.json():
            cache[query] = None
            return None
        hit = r.json()[0]
        lat, lng = float(hit["lat"]), float(hit["lon"])
        cache[query] = {"lat": lat, "lng": lng}
        return lat, lng
    except Exception as e:
        print(f"[warn] {query}: {e}", file=sys.stderr)
        return None


def main():
    cache = load_cache()
    features = []
    needs_network = False

    for e in ENTRIES:
        lat, lng = e.get("lat"), e.get("lng")
        if lat is None or lng is None:
            needs_network = True
            q = f"{e['name']}, {e['city']}, Spain"
            hit = geocode(q, cache)
            if not hit:
                print(f"[skip] no coords for {e['name']}", file=sys.stderr)
                continue
            lat, lng = hit
            e = {**e, "precision": e.get("precision", "approx")}
        features.append({
            "name": e["name"],
            "city": e["city"],
            "ccaa": e["ccaa"],
            "lat": round(float(lat), 6),
            "lng": round(float(lng), 6),
            "website": e.get("website"),
            "kind": e["kind"],
            "focus": e.get("focus", []),
            "languages": e.get("languages", []),
            "public": e.get("public"),
            "online": e.get("online", False),
            "offline": e.get("offline", True),
            "ages": e.get("ages", "both"),
            "note": e.get("note"),
            "precision": e.get("precision", "approx"),
            "verified": e.get("verified", False),
            "priority": e.get("priority", False),
        })

    if needs_network:
        save_cache(cache)

    payload = {
        "source": "Курация: публичные сайты госпиталей SNS (USMIJ) + ручная подборка частных мультиязычных и русскоязычных детских специалистов",
        "license": "Только публично опубликованные контакты учреждений и специалистов",
        "period": "2026-04",
        "category": "psych",
        "schema": {
            "name": "string",
            "city": "string",
            "ccaa": "string",
            "lat": "number",
            "lng": "number",
            "website": "url",
            "kind": "psicologo | neuropsicologo | psiquiatra | usmij",
            "focus": "массив из: cas | tea | tdah | ansiedad | general",
            "languages": "массив языковых кодов",
            "public": "boolean — true: бесплатно через SNS, false: частный",
            "online": "boolean",
            "offline": "boolean",
            "ages": "child | adolescent | both",
            "priority": "boolean — крупный маркер на карте",
            "verified": "boolean",
            "precision": "exact | approx",
        },
        "count": len(features),
        "features": features,
    }
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[ok] Wrote {OUT_PATH} ({size_kb:.1f} KB, {len(features)} entries)", file=sys.stderr)


if __name__ == "__main__":
    main()
