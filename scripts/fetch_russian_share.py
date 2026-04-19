"""
Fetch municipal-level share of Russian + Ukrainian residents from the INE
Estadística del Padrón Continuo (last published snapshot: 01-01-2022) and
build data/russian_share.json.

Rationale: data is pre-war-in-Ukraine (refugee wave started Feb 2022, not
captured in this snapshot). Relative rankings between municipios remain
informative, but absolute counts are understated by ~2x vs 2026 reality.

Source: 52 per-province "Población por sexo, municipios y nacionalidad
(principales nacionalidades)" tables in Tempus3. Madrid is table 33844,
others cluster at +6 ID steps (full list in RUS_TABLE_IDS).

Limitations:
- Only Rusia + Ucrania are exposed in "principales nacionalidades".
  Bielorrusia, Moldavia, Kazajstán, Kirguistán are not broken out, so this
  layer under-reports the total Russophone diaspora by ~15-20%.
- Rows without a total-population anchor for the municipio are skipped.

INE municipio codes come from data/municipalities_es.topojson (es-atlas,
CC-BY IGN). Matching is by (province_code, normalized_name) with fallback
to postpositive-article and bilingual-name variants (same logic as
fetch_salary_net.py).
"""
from __future__ import annotations

import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = ROOT / "data" / "russian_share.json"
TOPO_PATH = ROOT / "data" / "municipalities_es.topojson"

# Enumerated via a signature-scan of Tempus3 (tables matching the variable
# triple {Sexo, Municipios, Nacionalidad (principales nacionalidades)}). Two
# distinct clusters exist: 33572-33784 (step +6) covers the alphabetically
# earlier provinces, 33790-33976 (step +6) the later ones, + stragglers.
RUS_TABLE_IDS: list[int] = [
    33572, 33578, 33588, 33658,
    33688, 33694, 33700, 33706, 33712, 33718,
    33724, 33730, 33736, 33742, 33748, 33754,
    33760, 33766, 33772, 33778, 33784,
    33790, 33796, 33802, 33808, 33814, 33820, 33826, 33832, 33838, 33844,
    33850, 33856, 33862, 33868, 33874, 33880, 33886, 33892, 33898, 33904,
    33910, 33916, 33922, 33928, 33934, 33940, 33946,
    33952, 33958, 33964, 33970, 33976,
]

TEMPUS_BASE = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA"

# Rows we keep. Labels come as ". "-separated segments in arbitrary order.
# We filter on the Nacionalidad token and skip all rows unless Sex == Ambos / Total.
TARGET_NACS = ("Total", "Rusia", "Ucrania")
SEX_TOKENS = ("Total", "Ambos sexos")

_ARTICLES = ("el", "la", "los", "las", "l'", "els", "les", "lo", "o", "os", "a", "as")


def name_variants(raw: str) -> list[str]:
    """See fetch_salary_net.name_variants — identical logic, copied to keep
    the two fetchers independently runnable without a shared module."""
    s = raw.strip().replace("’", "'")
    halves = [p.strip() for p in s.split("/") if p.strip()]
    out: list[str] = []
    for half in halves:
        base = " ".join(half.lower().split())
        out.append(base)
        if ", " in base:
            head, tail = base.rsplit(", ", 1)
            if tail in _ARTICLES:
                sep = "" if tail.endswith("'") else " "
                out.append(f"{tail}{sep}{head}")
        for art in _ARTICLES:
            prefix = art if art.endswith("'") else art + " "
            if base.startswith(prefix):
                out.append(f"{base[len(prefix):]}, {art}")
                break
    return out


def load_gazetteer() -> tuple[dict[tuple[str, str], str], dict[str, list[str]]]:
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


class VolumeRestricted(Exception):
    """INE API refuses to serve tables above ~10 MB via the JSON endpoint.
    Raised so the main loop can log + skip without aborting the whole run."""


def fetch_table(table_id: int) -> list[dict]:
    url = f"{TEMPUS_BASE}/{table_id}?nult=1"
    resp = requests.get(url, headers={"User-Agent": "spain-map/fetch_russian_share"}, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    # INE error envelope: {"status": "No puede mostrarse por restricciones de volumen"}
    if isinstance(data, dict) and data.get("status"):
        raise VolumeRestricted(data["status"])
    return data


def parse_rows(rows: list[dict]) -> dict[str, dict[str, float]]:
    """
    Group rows by municipio name. Keep only rows where sex == "Total".
    For each muni accumulate {'Total': population, 'Rusia': n, 'Ucrania': n}.

    Row labels arrive with exactly 4 dot-separated segments, in one of two
    observed orderings depending on table vintage:
      format A:  "Dato base. {sex}. {municipio}. {nacionalidad}."
      format B:  "{sex}. {municipio}. {nacionalidad}. Dato base."
    The position of the "Dato base" sentinel disambiguates the order. This
    matters because "Total" appears both as a sex value AND as a nacionalidad
    value — positional parsing is the only way to tell them apart.
    """
    by_muni: dict[str, dict[str, float]] = defaultdict(dict)
    for r in rows:
        parts = [p.strip() for p in r.get("Nombre", "").split(".") if p.strip()]
        if len(parts) != 4:
            continue
        if parts[0] == "Dato base":
            _, sex, muni, nac = parts
        elif parts[-1] == "Dato base":
            sex, muni, nac, _ = parts
        else:
            continue
        if sex not in ("Total", "Ambos sexos"):
            continue
        if nac not in TARGET_NACS:
            continue
        data = r.get("Data") or []
        if not data:
            continue
        val = data[0].get("Valor")
        if val is None or data[0].get("Secreto"):
            continue
        by_muni[muni][nac] = float(val)
    return by_muni


def infer_province_code(names: list[str], name_provs: dict[str, list[str]]) -> str | None:
    counts: dict[str, int] = defaultdict(int)
    for name in names:
        for variant in name_variants(name):
            for prov in name_provs.get(variant, ()):
                counts[prov] += 1
    return max(counts, key=counts.get) if counts else None


def main() -> int:
    if not TOPO_PATH.exists():
        print(f"[fatal] {TOPO_PATH} not found", file=sys.stderr)
        return 2
    if not RUS_TABLE_IDS:
        print("[fatal] RUS_TABLE_IDS is empty — populate with scan results", file=sys.stderr)
        return 2

    gaz, name_provs = load_gazetteer()
    print(f"[gazetteer] {len(gaz)} (prov, name) entries", file=sys.stderr)

    all_munis: list[dict] = []
    seen_ine: set[str] = set()
    unmatched: list[tuple[int, str]] = []
    dropped_no_pop: int = 0
    provs_seen: set[str] = set()

    skipped_volume: list[int] = []
    for i, tid in enumerate(RUS_TABLE_IDS, 1):
        try:
            rows = fetch_table(tid)
        except VolumeRestricted as e:
            print(f"[skip] table {tid}: volume-restricted ({e})", file=sys.stderr)
            skipped_volume.append(tid)
            continue
        except Exception as e:
            print(f"[error] table {tid}: {e}", file=sys.stderr)
            continue

        by_muni = parse_rows(rows)
        if not by_muni:
            print(f"[warn] table {tid}: 0 muni groups", file=sys.stderr)
            continue

        prov_code = infer_province_code(list(by_muni.keys())[:25], name_provs)
        if not prov_code:
            print(f"[warn] table {tid}: could not infer province", file=sys.stderr)
            continue
        provs_seen.add(prov_code)

        matched = 0
        for name, vals in by_muni.items():
            pop = vals.get("Total")
            if pop is None or pop <= 0:
                dropped_no_pop += 1
                continue
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
            rus = vals.get("Rusia", 0.0)
            ukr = vals.get("Ucrania", 0.0)
            ru_uk = rus + ukr
            share_pct = round(ru_uk / pop * 100.0, 3) if pop > 0 else 0.0
            all_munis.append({
                "ine": ine,
                "name": name,
                "population": int(pop),
                "ru": int(rus),
                "uk": int(ukr),
                "ru_uk": int(ru_uk),
                "share_pct": share_pct,
            })
            matched += 1
        print(f"[{i:2d}/{len(RUS_TABLE_IDS)}] table={tid} prov={prov_code} "
              f"munis={len(by_muni)} matched={matched}", file=sys.stderr)
        time.sleep(0.2)

    print(f"\n[summary] {len(all_munis)} municipios "
          f"({len(provs_seen)} provinces) · "
          f"dropped-no-pop={dropped_no_pop} · unmatched={len(unmatched)} · "
          f"volume-skipped={len(skipped_volume)}", file=sys.stderr)

    pcts = sorted(m["share_pct"] for m in all_munis)
    if not pcts:
        print("[fatal] no values", file=sys.stderr)
        return 1
    p5 = pcts[max(0, int(len(pcts) * 0.05) - 1)]
    p95 = pcts[min(len(pcts) - 1, int(len(pcts) * 0.95))]

    payload = {
        "source": "INE Estadística del Padrón Continuo (01-01-2022, last snapshot)",
        "source_url": "https://www.ine.es/jaxiT3/Tabla.htm?t=33844",
        "period": "2022-01-01",
        "metric": "Доля граждан РФ+Украины от населения муниципалитета (%)",
        "granularity": "municipio",
        "count": len(all_munis),
        "legend_min": f"{p5:.2f}%",
        "legend_max": f"{p95:.2f}%",
        "legend_sub": (
            f"Padrón Continuo 2022 · {len(all_munis)} муниципалитетов · "
            f"только РУ+УК (без Белоруссии/Казахстана) · pre-war snapshot"
        ),
        "stats": {
            "min_pct": pcts[0],
            "max_pct": pcts[-1],
            "p5_pct": p5,
            "p95_pct": p95,
        },
        "missing_provinces": [],
        "municipios": sorted(all_munis, key=lambda m: m["ine"]),
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[ok] Wrote {OUT_PATH} ({size_kb:.1f} KB, {len(all_munis)} municipios)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
