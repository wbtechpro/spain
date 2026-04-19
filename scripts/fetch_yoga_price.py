"""
Build data/yoga_price.json — median price of a 60-min group yoga class
(drop-in studio price) for major Spanish cities.

Source: Cronoshare "¿Cuánto cuesta una clase de yoga?" (2025 update),
section "Precios orientativos por ciudad (clases de yoga, 60 min)". Values are
the midpoint of the published range for *group* classes per person — the
realistic drop-in price a relocant actually pays, not private-lesson rates.

Only 7 provinces are populated; the remaining 45 are left as missing (grey on
the map) rather than extrapolated — the dataset doesn't support Spain-wide
coverage honestly.
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from spain_provinces import PROVINCES, find as find_province  # noqa: E402

OUT_PATH = ROOT / "data" / "yoga_price.json"

# province_es → (group_class_min, group_class_max) in €/час
CITY_PRICES: dict[str, tuple[int, int]] = {
    "Madrid":      (10, 18),
    "Barcelona":   (10, 17),
    "Bilbao":      (9,  15),   # province: Bizkaia
    "Valencia":    (8,  14),
    "Málaga":      (8,  14),
    "Zaragoza":    (8,  13),
    "Sevilla":     (7,  12),
}

# City → province for lookup (Bilbao lives in Bizkaia, etc.)
CITY_TO_PROVINCE_ES: dict[str, str] = {
    "Madrid":    "Madrid",
    "Barcelona": "Barcelona",
    "Bilbao":    "Bizkaia",
    "Valencia":  "Valencia",
    "Málaga":    "Málaga",
    "Zaragoza":  "Zaragoza",
    "Sevilla":   "Sevilla",
}


def main() -> int:
    matched = []
    for city, (lo, hi) in CITY_PRICES.items():
        prov_name = CITY_TO_PROVINCE_ES[city]
        p = find_province(prov_name)
        if not p:
            print(f"[warn] no province found for {city} → {prov_name}", file=sys.stderr)
            continue
        ru, es, ine, lat, lng = p
        midpoint = round((lo + hi) / 2, 1)
        matched.append({
            "ine": ine,
            "province_ru": ru,
            "province_es": es,
            "lat": lat,
            "lng": lng,
            "city": city,
            "price_eur_min": lo,
            "price_eur_max": hi,
            "price_eur_median": midpoint,
        })

    matched.sort(key=lambda m: m["ine"])

    values = sorted(m["price_eur_median"] for m in matched)
    vmin, vmax = values[0], values[-1]

    def intensity(v: float) -> float:
        if vmax == vmin:
            return 0.5
        return round((v - vmin) / (vmax - vmin), 3)

    points = [
        [m["lat"], m["lng"], intensity(m["price_eur_median"]), m["price_eur_median"]]
        for m in matched
    ]

    covered = {m["province_es"] for m in matched}
    missing = [es for (_ru, es, *_rest) in PROVINCES if es not in covered]

    payload = {
        "source": "Cronoshare · ¿Cuánto cuesta una clase de yoga? (2025)",
        "source_url": "https://www.cronoshare.com/cuanto-cuesta/clase-yoga",
        "period": "2025",
        "metric": "цена групповой часовой йоги, медиана диапазона (€/час)",
        "granularity": "город (7 крупных)",
        "count": len(points),
        "legend_min": f"{vmin} €/час",
        "legend_max": f"{vmax} €/час",
        "legend_sub": f"Cronoshare · 2025 · {len(matched)}/52 провинций (остальные н.д.)",
        "render_radius": 55,
        "render_blur": 45,
        "stats": {
            "min_eur": vmin,
            "max_eur": vmax,
        },
        "missing_provinces": missing,
        "provinces": matched,
        "points": points,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[ok] Wrote {OUT_PATH} ({size_kb:.1f} KB, {len(points)} provinces, {len(missing)} missing)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
