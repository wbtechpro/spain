"""
Build data/cita_previa.json — waiting days for first specialist consultation
(primera consulta externa) by Spanish province.

Source: SISLE-SNS (Sistema de Información sobre Listas de Espera en el SNS),
Ministerio de Sanidad. Situación a 31 de diciembre de 2024, datos por
comunidad autónoma.

Values are reported per CCAA; we broadcast each CCAA's number to all provinces
inside it, so the choropleth covers all 52 provinces.

No PDF/API scraping — data is static and hand-transcribed from the published
summary. Re-run after SISLE publishes a newer edition; just bump the dict and
`period` below.
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from spain_provinces import PROVINCES  # noqa: E402

OUT_PATH = ROOT / "data" / "cita_previa.json"

# Days to first specialist consultation by CCAA, SISLE dec-2024.
# https://www.sanidad.gob.es/estadEstudios/estadisticas/inforRecopilaciones/listaEsperaInfCCAA.htm
CCAA_DAYS: dict[str, int] = {
    "andalucia":         150,
    "aragon":            128,
    "asturias":           97,
    "baleares":           84,
    "canarias":          157,
    "cantabria":          77,
    "castilla_leon":      89,
    "castilla_mancha":    60,
    "cataluna":          110,
    "valenciana":         93,
    "extremadura":       125,
    "galicia":            61,
    "madrid":             72,
    "murcia":             97,
    "navarra":           154,
    "pais_vasco":         43,
    "rioja":              79,
    "ceuta":              83,
    "melilla":            23,
}

# Province INE → CCAA key
PROV_TO_CCAA: dict[str, str] = {
    "01": "pais_vasco", "48": "pais_vasco", "20": "pais_vasco",
    "02": "castilla_mancha", "13": "castilla_mancha", "16": "castilla_mancha", "19": "castilla_mancha", "45": "castilla_mancha",
    "03": "valenciana", "12": "valenciana", "46": "valenciana",
    "04": "andalucia", "11": "andalucia", "14": "andalucia", "18": "andalucia", "21": "andalucia", "23": "andalucia", "29": "andalucia", "41": "andalucia",
    "05": "castilla_leon", "09": "castilla_leon", "24": "castilla_leon", "34": "castilla_leon", "37": "castilla_leon", "40": "castilla_leon", "42": "castilla_leon", "47": "castilla_leon", "49": "castilla_leon",
    "06": "extremadura", "10": "extremadura",
    "07": "baleares",
    "08": "cataluna", "17": "cataluna", "25": "cataluna", "43": "cataluna",
    "15": "galicia", "27": "galicia", "32": "galicia", "36": "galicia",
    "22": "aragon", "44": "aragon", "50": "aragon",
    "26": "rioja",
    "28": "madrid",
    "30": "murcia",
    "31": "navarra",
    "33": "asturias",
    "35": "canarias", "38": "canarias",
    "39": "cantabria",
    "51": "ceuta",
    "52": "melilla",
}


def main() -> int:
    matched = []
    for ru, es, ine, lat, lng, _aliases in PROVINCES:
        ccaa = PROV_TO_CCAA.get(ine)
        if ccaa is None:
            print(f"[warn] province {ine} {es}: no CCAA mapping", file=sys.stderr)
            continue
        days = CCAA_DAYS[ccaa]
        matched.append({
            "ine": ine,
            "province_ru": ru,
            "province_es": es,
            "lat": lat,
            "lng": lng,
            "ccaa_key": ccaa,
            "wait_days": days,
        })

    values = sorted(m["wait_days"] for m in matched)
    vmin, vmax = values[0], values[-1]

    def intensity(v: int) -> float:
        if vmax == vmin:
            return 0.5
        return round((v - vmin) / (vmax - vmin), 3)

    points = [
        [m["lat"], m["lng"], intensity(m["wait_days"]), m["wait_days"]]
        for m in matched
    ]

    payload = {
        "source": "SISLE-SNS · Ministerio de Sanidad",
        "source_url": "https://www.sanidad.gob.es/estadEstudios/estadisticas/inforRecopilaciones/listaEsperaInfCCAA.htm",
        "period": "2024-12-31",
        "metric": "días de espera para primera consulta con especialista (primera consulta externa)",
        "granularity": "comunidad autónoma (broadcast a provincia)",
        "count": len(points),
        "legend_min": f"{vmin} дн.",
        "legend_max": f"{vmax} дн.",
        "legend_sub": f"SISLE · дек 2024 · {len(CCAA_DAYS)} CCAA → {len(matched)} провинций",
        "render_radius": 55,
        "render_blur": 45,
        "stats": {
            "min_days": vmin,
            "max_days": vmax,
            "national_avg_days": 105,
        },
        "missing_provinces": [],
        "provinces": matched,
        "points": points,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[ok] Wrote {OUT_PATH} ({size_kb:.1f} KB, {len(points)} provinces)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
