"""
Fetch municipal-level net household income from INE ADRH (Atlas de Distribución
de Renta de los Hogares) and build data/salary_net_median.json.

Indicator: "Mediana de la renta por unidad de consumo" (median net disposable
income per equivalent adult, after taxes, OECD-modified equivalence scale).

Source: INE ADRH, operación 353. One per-province table per autonomía fiscal
common (~48 tables covering 50 state-tax provinces; Basque Country + Navarra
are omitted — they file with haciendas forales that INE cannot aggregate).

INE codes (5-digit municipal code) are taken from data/municipalities_es.topojson
(es-atlas, CC-BY IGN); ADRH rows carry only the municipio name, so we match on
(province_code, name) with the topojson serving as the authoritative gazetteer.
"""
from __future__ import annotations
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = ROOT / "data" / "salary_net_median.json"
TOPO_PATH = ROOT / "data" / "municipalities_es.topojson"

# Catalog page: https://www.ine.es/dynt3/inebase/index.htm?padre=7132
# 52 per-province "Indicadores de renta media y mediana" tables — every state
# province + Ceuta + Melilla. ADRH does include País Vasco + Navarra despite
# their autonomous Hacienda Foral: INE receives aggregated returns via convenio.
RENTA_TABLE_IDS: list[int] = [
    30656, 30833, 30842, 30851, 30860, 30869, 30878, 30887,
    30896, 30917, 30926, 30935, 30944, 30953, 30962, 30971,
    30980, 30989, 30998, 31007, 31016, 31025, 31034, 31043,
    31052, 31061, 31070, 31079, 31088, 31097, 31106, 31115,
    31124, 31133, 31142, 31151, 31160, 31169, 31178, 31187,
    31196, 31205, 31214, 31223, 31232, 31241, 31250, 31259,
    31268, 31277, 31286, 31295,
]

INDICATOR_NAME = "Mediana de la renta por unidad de consumo"
TEMPUS_BASE = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA"

# All 52 provinces are covered via ADRH; no geographic gaps in the source.
MISSING_PROVINCES: list[str] = []


_ARTICLES = ("el", "la", "los", "las", "l'", "els", "les", "lo", "o", "os", "a", "as")


def name_variants(raw: str) -> list[str]:
    """
    Generate normalized name variants to bridge the two INE label formats:
      - "{Name}, {Article}"   → INE/AEAT tabular style
      - "{Article} {Name}"    → es-atlas/cartographic style
      - "{Name}/{Alt}"        → bilingual (Valencian/Catalan/Galician/Euskera)
    Also strips typographical apostrophes/spaces so "l'Alfàs" == "L'Alfàs".
    """
    s = raw.strip().replace("’", "'")
    # split bilingual pairs: "Poble Nou de Benitatxell, el/Benitachell"
    halves = [p.strip() for p in s.split("/") if p.strip()]
    out: list[str] = []
    for half in halves:
        # canonical normalization
        base = " ".join(half.lower().split())
        out.append(base)
        # "{Name}, {Article}" → "{Article} {Name}"
        if ", " in base:
            head, tail = base.rsplit(", ", 1)
            if tail in _ARTICLES:
                # "l'" attaches without space; others separate by space
                sep = "" if tail.endswith("'") else " "
                out.append(f"{tail}{sep}{head}")
        # "{Article} {Name}" → "{Name}, {Article}"
        for art in _ARTICLES:
            prefix = art if art.endswith("'") else art + " "
            if base.startswith(prefix):
                out.append(f"{base[len(prefix):]}, {art}")
                break
    return out


def normalize(s: str) -> str:
    return name_variants(s)[0]


def load_gazetteer() -> tuple[dict[tuple[str, str], str], dict[str, list[str]]]:
    """
    Build two indices from es-atlas topojson:
      gaz:        {(province_code, normalized_name) → ine_code} — O(1) final lookup
      name_provs: {normalized_name → [province_code, ...]}      — O(1) province vote
    Every generated name variant is indexed so either spelling convention matches.
    """
    topo = json.loads(TOPO_PATH.read_text(encoding="utf-8"))
    munis = topo["objects"]["municipalities"]["geometries"]
    gaz: dict[tuple[str, str], str] = {}
    name_provs: dict[str, list[str]] = defaultdict(list)
    for g in munis:
        ine = g.get("id") or ""
        name = (g.get("properties") or {}).get("name", "")
        if len(ine) != 5 or not name:
            continue
        prov = ine[:2]
        for variant in name_variants(name):
            gaz.setdefault((prov, variant), ine)
            name_provs[variant].append(prov)
    return gaz, name_provs


def fetch_table(table_id: int) -> list[dict]:
    url = f"{TEMPUS_BASE}/{table_id}?nult=1"
    resp = requests.get(url, headers={"User-Agent": "spain-map/fetch_salary_net"}, timeout=60)
    resp.raise_for_status()
    return resp.json()


def parse_rows(rows: list[dict]) -> list[tuple[str, float]]:
    """
    Extract (municipio_name, value_eur) pairs for indicator "Mediana de la renta
    por unidad de consumo". We drop census-district breakdowns: names containing
    "distrito" or "sección" indicate sub-municipal rows.
    """
    out: list[tuple[str, float]] = []
    for r in rows:
        nombre = r.get("Nombre", "")
        if INDICATOR_NAME not in nombre:
            continue
        if "distrito" in nombre.lower() or "sección" in nombre.lower():
            continue
        # Row format: "{Municipio}. Dato base. Mediana de la renta por unidad de consumo."
        parts = [p.strip() for p in nombre.split(".") if p.strip()]
        if not parts:
            continue
        # municipio is the first segment (before "Dato base")
        muni = parts[0]
        data = r.get("Data") or []
        if not data:
            continue
        val = data[0].get("Valor")
        if val is None or data[0].get("Secreto"):
            continue
        out.append((muni, float(val)))
    return out


def infer_province_code(names: list[str], name_provs: dict[str, list[str]]) -> str | None:
    """
    Given a list of municipio names from one ADRH table, determine which
    province they belong to by tallying INE prefix matches.
    """
    counts: dict[str, int] = defaultdict(int)
    for name in names:
        for variant in name_variants(name):
            for prov in name_provs.get(variant, ()):
                counts[prov] += 1
    if not counts:
        return None
    return max(counts, key=counts.get)


def main() -> int:
    if not TOPO_PATH.exists():
        print(f"[fatal] {TOPO_PATH} not found", file=sys.stderr)
        return 2

    gaz, name_provs = load_gazetteer()
    print(f"[gazetteer] {len(gaz)} (province, name) entries, "
          f"{len(name_provs)} unique names from es-atlas", file=sys.stderr)

    all_munis: list[dict] = []
    seen_ine: set[str] = set()
    unmatched: list[tuple[int, str]] = []
    provs_seen: set[str] = set()

    for i, tid in enumerate(RENTA_TABLE_IDS, 1):
        try:
            rows = fetch_table(tid)
        except Exception as e:
            print(f"[error] table {tid}: {e}", file=sys.stderr)
            continue

        pairs = parse_rows(rows)
        if not pairs:
            print(f"[warn] table {tid}: 0 rows after filter", file=sys.stderr)
            continue

        prov_code = infer_province_code([n for n, _ in pairs[:20]], name_provs)
        if not prov_code:
            print(f"[warn] table {tid}: could not infer province", file=sys.stderr)
            continue
        provs_seen.add(prov_code)

        matched = 0
        for name, value in pairs:
            ine = None
            for variant in name_variants(name):
                ine = gaz.get((prov_code, variant))
                if ine:
                    break
            if not ine:
                unmatched.append((tid, name))
                continue
            if ine in seen_ine:
                continue
            seen_ine.add(ine)
            all_munis.append({
                "ine": ine,
                "name": name,
                "value_eur": round(value, 0),
            })
            matched += 1
        print(f"[{i:2d}/{len(RENTA_TABLE_IDS)}] table={tid} prov={prov_code} "
              f"rows={len(pairs)} matched={matched}", file=sys.stderr)
        time.sleep(0.2)

    print(f"\n[summary] {len(all_munis)} municipios with data "
          f"({len(provs_seen)} provinces)", file=sys.stderr)
    if unmatched:
        print(f"[unmatched] {len(unmatched)} name mismatches (first 10):", file=sys.stderr)
        for tid, name in unmatched[:10]:
            print(f"  table {tid}: {name!r}", file=sys.stderr)

    values = sorted(m["value_eur"] for m in all_munis)
    if not values:
        print("[fatal] no values collected", file=sys.stderr)
        return 1

    p5 = values[max(0, int(len(values) * 0.05) - 1)]
    p95 = values[min(len(values) - 1, int(len(values) * 0.95))]

    payload = {
        "source": "INE ADRH 2023 (Atlas de Distribución de Renta de los Hogares)",
        "source_url": "https://www.ine.es/dynt3/inebase/index.htm?padre=7132",
        "period": "2023 (declaraciones tributarias, AEAT vía INE)",
        "metric": "Mediana de la renta por unidad de consumo (€/год на эквивалентного взрослого)",
        "granularity": "municipio",
        "count": len(all_munis),
        "legend_min": f"{round(p5):,} €".replace(",", " "),
        "legend_max": f"{round(p95):,} €".replace(",", " "),
        "legend_sub": (
            f"ADRH 2023 · медиана располагаемого дохода (после налогов) · "
            f"{len(all_munis)} муниципалитетов"
        ),
        "stats": {
            "min_eur": values[0],
            "max_eur": values[-1],
            "p5_eur": p5,
            "p95_eur": p95,
        },
        "missing_provinces": MISSING_PROVINCES,
        "municipios": sorted(all_munis, key=lambda m: m["ine"]),
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[ok] Wrote {OUT_PATH} ({size_kb:.1f} KB, {len(all_munis)} municipios)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
