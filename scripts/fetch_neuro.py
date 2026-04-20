"""
Build a curated JSON of pediatric neurologists in Spain.

Pediatric neurology is the diagnostic gateway for CAS and other
neurodevelopmental conditions: pediatra → neurólogo infantil → SLP / NEAE
school / USMIJ. Different from the `psych` layer (psicólogo / psiquiatra),
different from `hospitals` (general). Different from `psych` because
neurologist *diagnoses and rules out* (epilepsy, genetic syndromes),
psychiatrist *treats* (meds, therapy).

Two slices:
  - kind=departamento     → Sección de Neurología Pediátrica at major SNS
                            hospitals (free with primary-care referral).
  - kind=consulta_privada → private hospitals/clinics whose public cuadro
                            médico explicitly lists "neurología pediátrica".

Output: data/neuro.json
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = ROOT / "data" / "neuro.json"
CACHE_PATH = ROOT / "data" / "raw" / "neuro_geocode_cache.json"

NOMINATIM = "https://nominatim.openstreetmap.org/search"
UA = "spain-map-relocation/0.1 (curation)"

# Schema mirrors data/psych.json but with neuro-specific kinds and focus tags:
#   kind:   "departamento" (public SNS) | "consulta_privada"
#   focus:  list from {cas, epilepsia, desarrollo, tea, general}
#   public: true (free via SNS) | false (private)
#   ages:   "child" | "adolescent" | "both"
ENTRIES: list[dict] = [
    # ============================================================
    # PUBLIC — Neurología Pediátrica at major SNS hospitals
    # These are named units within hospitals; the parent hospital
    # itself shows up in the `hospitals` layer, but the unit here
    # is what matters for the diagnostic gateway.
    # ============================================================
    {
        "name": "Niño Jesús — Sección de Neurología Pediátrica",
        "city": "Madrid", "ccaa": "Madrid",
        "lat": 40.4117, "lng": -3.6730,
        "website": "https://www.comunidad.madrid/hospital/ninojesus/profesionales/cartera-servicios/seccion-neurologia",
        "kind": "departamento",
        "focus": ["general", "cas", "epilepsia", "desarrollo", "tea"],
        "languages": ["es"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "note": "Референсный детский госпиталь Мадрида. Полный спектр: эпилепсия, нейроразвитие, генетика.",
        "precision": "exact", "verified": True, "priority": True,
    },
    {
        "name": "Sant Joan de Déu — Servicio de Neurología Pediátrica",
        "city": "Esplugues de Llobregat", "ccaa": "Catalunya",
        "lat": 41.3740, "lng": 2.0878,
        "website": "https://www.sjdhospitalbarcelona.org/es/neurologia",
        "kind": "departamento",
        "focus": ["general", "cas", "epilepsia", "desarrollo", "tea"],
        "languages": ["es", "ca"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "note": "Одно из сильнейших отделений детской неврологии в Европе.",
        "precision": "exact", "verified": True, "priority": True,
    },
    {
        "name": "Vall d'Hebron — Neurología Pediátrica",
        "city": "Barcelona", "ccaa": "Catalunya",
        "lat": 41.4271, "lng": 2.1490,
        "website": "https://www.vallhebron.com/es/asistencia/cartera-de-servicios/neurologia-pediatrica",
        "kind": "departamento",
        "focus": ["general", "epilepsia", "desarrollo"],
        "languages": ["es", "ca"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "La Paz — Servicio de Neurología Pediátrica",
        "city": "Madrid", "ccaa": "Madrid",
        "lat": 40.4796, "lng": -3.6913,
        "website": "https://www.comunidad.madrid/hospital/lapaz/",
        "kind": "departamento",
        "focus": ["general", "epilepsia", "desarrollo"],
        "languages": ["es"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "12 de Octubre — Neurología Pediátrica",
        "city": "Madrid", "ccaa": "Madrid",
        "lat": 40.3722, "lng": -3.6991,
        "website": "https://www.comunidad.madrid/hospital/12octubre/",
        "kind": "departamento",
        "focus": ["general", "epilepsia"],
        "languages": ["es"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "Gregorio Marañón — Neuropediatría",
        "city": "Madrid", "ccaa": "Madrid",
        "lat": 40.4205, "lng": -3.6675,
        "website": "https://www.comunidad.madrid/hospital/gregoriomaranon/",
        "kind": "departamento",
        "focus": ["general", "epilepsia"],
        "languages": ["es"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "Hospital La Fe — Neuropediatría",
        "city": "Valencia", "ccaa": "Valenciana",
        "lat": 39.4427, "lng": -0.3784,
        "website": "https://www.lafe.san.gva.es/",
        "kind": "departamento",
        "focus": ["general", "epilepsia", "desarrollo"],
        "languages": ["es"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "Virgen del Rocío — Neuropediatría",
        "city": "Sevilla", "ccaa": "Andalucía",
        "lat": 37.3580, "lng": -5.9846,
        "website": "https://www.hospitalesvirgendelrocio.es/",
        "kind": "departamento",
        "focus": ["general", "epilepsia"],
        "languages": ["es"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "Hospital Regional de Málaga — Neuropediatría",
        "city": "Málaga", "ccaa": "Andalucía",
        "lat": 36.7155, "lng": -4.4470,
        "website": "https://www.hospitalregionaldemalaga.es/",
        "kind": "departamento",
        "focus": ["general", "epilepsia"],
        "languages": ["es"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "Cruces — Neuropediatría",
        "city": "Barakaldo", "ccaa": "País Vasco",
        "lat": 43.2891, "lng": -2.9939,
        "website": "https://www.osakidetza.euskadi.eus/hospital-universitario-cruces/",
        "kind": "departamento",
        "focus": ["general", "epilepsia"],
        "languages": ["es", "eu"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "Son Espases — Neuropediatría",
        "city": "Palma de Mallorca", "ccaa": "Illes Balears",
        "lat": 39.5959, "lng": 2.6359,
        "website": "https://www.hsll.es/",
        "kind": "departamento",
        "focus": ["general", "epilepsia"],
        "languages": ["es", "ca"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },
    {
        "name": "CHUAC (A Coruña) — Neuropediatría",
        "city": "A Coruña", "ccaa": "Galicia",
        "lat": 43.3380, "lng": -8.4115,
        "website": "https://xxiacoruna.sergas.gal/",
        "kind": "departamento",
        "focus": ["general", "epilepsia"],
        "languages": ["es", "gl"],
        "public": True, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": True,
    },

    # ============================================================
    # PRIVATE — hospitals whose public cuadro médico explicitly
    # lists "neurología pediátrica" as a service.
    # Marked verified: false because individual specialists rotate;
    # the institution exists, the service is listed, but specific
    # doctor identity needs confirmation when booking.
    # ============================================================
    {
        "name": "Hospital Ruber Internacional — Neurología Pediátrica",
        "city": "Madrid", "ccaa": "Madrid",
        "lat": 40.4709, "lng": -3.6781,
        "website": "https://www.ruberinternacional.es/",
        "kind": "consulta_privada",
        "focus": ["general", "epilepsia", "desarrollo"],
        "languages": ["es", "en"],
        "public": False, "online": False, "offline": True,
        "ages": "both",
        "note": "Частный референсный госпиталь, есть сильное отделение детской неврологии.",
        "precision": "exact", "verified": False,
    },
    {
        "name": "Quirónsalud Madrid — Unidad de Neuropediatría",
        "city": "Pozuelo de Alarcón", "ccaa": "Madrid",
        "lat": 40.4385, "lng": -3.7969,
        "website": "https://www.quironsalud.com/madrid/",
        "kind": "consulta_privada",
        "focus": ["general", "epilepsia", "desarrollo"],
        "languages": ["es", "en"],
        "public": False, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": False,
    },
    {
        "name": "HM Montepríncipe — Neurología Infantil",
        "city": "Boadilla del Monte", "ccaa": "Madrid",
        "lat": 40.4027, "lng": -3.8732,
        "website": "https://www.hmhospitales.com/montepríncipe",
        "kind": "consulta_privada",
        "focus": ["general", "desarrollo"],
        "languages": ["es"],
        "public": False, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": False,
    },
    {
        "name": "Hospital Quirónsalud Barcelona — Neuropediatría",
        "city": "Barcelona", "ccaa": "Catalunya",
        "lat": 41.4015, "lng": 2.1339,
        "website": "https://www.quironsalud.com/barcelona/",
        "kind": "consulta_privada",
        "focus": ["general", "epilepsia", "desarrollo"],
        "languages": ["es", "ca", "en"],
        "public": False, "online": False, "offline": True,
        "ages": "both",
        "precision": "exact", "verified": False,
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
        "source": "Курация: публичные сайты госпиталей SNS (отделения Neurología Pediátrica) + крупные частные клиники с явно указанной услугой",
        "license": "Только публично опубликованные данные учреждений",
        "period": "2026-04",
        "category": "neuro",
        "schema": {
            "name": "string",
            "city": "string",
            "ccaa": "string",
            "lat": "number",
            "lng": "number",
            "website": "url",
            "kind": "departamento | consulta_privada",
            "focus": "массив из: cas | epilepsia | desarrollo | tea | general",
            "languages": "массив языковых кодов",
            "public": "boolean — true: бесплатно через SNS",
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
