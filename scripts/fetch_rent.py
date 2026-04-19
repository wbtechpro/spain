"""
Parse SERPAVI 2025 PDF (Ministerio de Vivienda y Agenda Urbana) and extract
median monthly rent per province.

Source: https://publicaciones.transportes.gob.es/sistema-estatal-referencia-precio-alquiler-vivienda-serpavi-2025
Table:  "Tabla 1. Renta media (€/m2 mes), superficie construida media (m2) y
         cuantía media (€/mes) ... año 2023 — comunidades autónomas y provincias"
License: OpenData compatible, MITMA/MIVAU attribution required.

Output: data/rent_cost.json
"""
from __future__ import annotations
import json
import re
import sys
import subprocess
from pathlib import Path

import pdfplumber

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from spain_provinces import PROVINCES, find as find_province  # noqa: E402

PDF_URL = "https://publicaciones.transportes.gob.es/downloadcustom/sample/3953"
PDF_PATH = ROOT / "data" / "raw" / "serpavi_2025.pdf"
OUT_PATH = ROOT / "data" / "rent_cost.json"

# Page 5 of SERPAVI 2025 holds the provincial summary table.
TABLE_PAGE = 4   # 0-indexed


def download_if_missing():
    if PDF_PATH.exists() and PDF_PATH.stat().st_size > 100_000:
        return
    PDF_PATH.parent.mkdir(parents=True, exist_ok=True)
    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0"
    print(f"[fetch] downloading {PDF_URL}", file=sys.stderr)
    subprocess.run(
        ["curl", "-sL", "-A", ua, "-e", "https://transportes.gob.es/", PDF_URL, "-o", str(PDF_PATH)],
        check=True,
    )


# Each province row looks like:
#   "Almería 19.666 5,6 4,3 7,1 82 67 100 425 350 545"
#   name = leading non-digit tokens (may have slashes, spaces, parentheses)
#   followed by 10 numeric fields: testigos | rent €/m² (M, P25, P75)
#                                  | size (M, P25, P75) | €/mo (M, P25, P75)
ROW_RE = re.compile(
    r"""^
    (?P<name>[A-Za-zÀ-ÿ()/\s\.\-]+?)              # province name (any Latin-1 letter)
    \s+
    (?P<testigos>[\d\.]+|n\.d\.)                  # sample size or n.d.
    \s+
    (?P<rent_m2_m>[\d,]+|n\.d\.)    \s+
    (?P<rent_m2_p25>[\d,]+|n\.d\.)  \s+
    (?P<rent_m2_p75>[\d,]+|n\.d\.)  \s+
    (?P<size_m>[\d,]+|n\.d\.)       \s+
    (?P<size_p25>[\d,]+|n\.d\.)     \s+
    (?P<size_p75>[\d,]+|n\.d\.)     \s+
    (?P<rent_mo_m>[\d,\.]+|n\.d\.)  \s+
    (?P<rent_mo_p25>[\d,\.]+|n\.d\.) \s+
    (?P<rent_mo_p75>[\d,\.]+|n\.d\.)
    \s*$
    """,
    re.VERBOSE,
)


def to_num(s: str) -> float | None:
    if not s or s.lower() == "n.d.":
        return None
    # Spanish format: "1.234,56" → 1234.56
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def parse_table(pdf_path: Path) -> list[dict]:
    rows = []
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[TABLE_PAGE]
        text = page.extract_text() or ""

    for line in text.splitlines():
        m = ROW_RE.match(line.strip())
        if not m:
            continue
        name = m.group("name").strip()
        # skip aggregates (comunidades autónomas are also in the table)
        rent_mo = to_num(m.group("rent_mo_m"))
        rent_m2 = to_num(m.group("rent_m2_m"))
        size = to_num(m.group("size_m"))
        if rent_mo is None:
            continue
        rows.append({
            "raw_name": name,
            "rent_eur_month_median": rent_mo,
            "rent_eur_m2_median": rent_m2,
            "size_m2_median": size,
            "sample_size": int((m.group("testigos") or "0").replace(".", "")) if m.group("testigos") != "n.d." else 0,
        })
    return rows


def main() -> int:
    download_if_missing()
    raw_rows = parse_table(PDF_PATH)
    print(f"[parse] matched {len(raw_rows)} rows in SERPAVI table", file=sys.stderr)

    # Keep only rows that map to a known province (filters out CCAA aggregates and Spain total).
    matched = []
    for r in raw_rows:
        p = find_province(r["raw_name"])
        if not p:
            print(f"[skip] not a province: {r['raw_name']}", file=sys.stderr)
            continue
        ru, es, ine, lat, lng = p
        matched.append({
            "ine": ine,
            "province_ru": ru,
            "province_es": es,
            "lat": lat,
            "lng": lng,
            **{k: v for k, v in r.items() if k != "raw_name"},
        })

    print(f"[match] {len(matched)} provinces mapped", file=sys.stderr)
    missing = [es for (_ru, es, *_rest) in PROVINCES if not any(m["province_es"] == es for m in matched)]
    if missing:
        print(f"[warn] {len(missing)} provinces without data: {missing}", file=sys.stderr)

    # Normalize rent_eur_month to 0-1 intensity using p5/p95
    values = sorted(m["rent_eur_month_median"] for m in matched if m["rent_eur_month_median"])
    p5 = values[max(0, int(len(values) * 0.05) - 1)]
    p95 = values[min(len(values) - 1, int(len(values) * 0.95))]

    def intensity(v):
        if p95 == p5:
            return 0.5
        return max(0.0, min(1.0, (v - p5) / (p95 - p5)))

    points = [
        [m["lat"], m["lng"], round(intensity(m["rent_eur_month_median"]), 3), round(m["rent_eur_month_median"], 1)]
        for m in matched
    ]

    payload = {
        "source": "SERPAVI 2025 (Ministerio de Vivienda)",
        "source_url": "https://publicaciones.transportes.gob.es/sistema-estatal-referencia-precio-alquiler-vivienda-serpavi-2025",
        "period": "2023 (declaraciones tributarias)",
        "metric": "renta media mensual del arrendamiento (€/mes)",
        "granularity": "provincia",
        "count": len(points),
        "legend_min": f"{round(p5)} €/мес",
        "legend_max": f"{round(p95)} €/мес",
        "legend_sub": f"SERPAVI 2023 · {len(matched)}/52 провинций (Страна Басков н.д.)",
        "render_radius": 55,
        "render_blur": 45,
        "stats": {
            "min_eur": round(min(values), 1),
            "max_eur": round(max(values), 1),
            "p5_eur": round(p5, 1),
            "p95_eur": round(p95, 1),
        },
        "missing_provinces": missing,
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
