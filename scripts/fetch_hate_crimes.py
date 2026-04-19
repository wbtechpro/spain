"""
Parse the Ministerio del Interior 2024 hate-crimes report PDF and extract
provincial counts of known criminal infractions (hechos conocidos).

Source: https://interior.gob.es/opencms/export/sites/default/.galleries/galeria-de-prensa/documentos-y-multimedia/balances-e-informes/2024/INFORME_Evolucion_delitos_de_odio_2024.pdf
Tables: §7.2 "Datos de HECHOS CONOCIDOS desagregados por provincias" (pages 43-44)
License: public (Spanish government publication).

Output: data/hate_crimes.json
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

PDF_URL = "https://interior.gob.es/opencms/export/sites/default/.galleries/galeria-de-prensa/documentos-y-multimedia/balances-e-informes/2024/INFORME_Evolucion_delitos_de_odio_2024.pdf"
PDF_PATH = ROOT / "data" / "raw" / "delitos_odio_2024.pdf"
OUT_PATH = ROOT / "data" / "hate_crimes.json"

TABLE_PAGES = [42, 43]   # 0-indexed: pages 43, 44 of the PDF

# Row format: "<province name> <total> <n1> <n2> ... <n13>"
# 14 numeric columns after the name. First column is TOTAL hechos conocidos.
ROW_RE = re.compile(
    r"""^
    (?P<name>[A-Za-zÀ-ÿ()/\s\.\-]+?)
    \s+
    (?P<total>\d+)
    (?:\s+\d+){13}
    \s*$
    """,
    re.VERBOSE,
)

# rows to skip (not provinces)
SKIP_NAMES = {"en el extranjero", "desconocido", "españa", "total"}


def download_if_missing():
    if PDF_PATH.exists() and PDF_PATH.stat().st_size > 100_000:
        return
    PDF_PATH.parent.mkdir(parents=True, exist_ok=True)
    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0"
    print(f"[fetch] downloading {PDF_URL}", file=sys.stderr)
    subprocess.run(
        ["curl", "-sL", "-A", ua, "-e", "https://interior.gob.es/", PDF_URL, "-o", str(PDF_PATH)],
        check=True,
    )


def parse_pdf(pdf_path: Path) -> list[dict]:
    rows = []
    with pdfplumber.open(pdf_path) as pdf:
        for pi in TABLE_PAGES:
            text = pdf.pages[pi].extract_text() or ""
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                m = ROW_RE.match(line)
                if not m:
                    continue
                name = m.group("name").strip()
                if name.lower() in SKIP_NAMES:
                    continue
                rows.append({"raw_name": name, "total": int(m.group("total"))})
    return rows


def main() -> int:
    download_if_missing()
    raw = parse_pdf(PDF_PATH)
    print(f"[parse] matched {len(raw)} rows", file=sys.stderr)

    matched = []
    for r in raw:
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
            "hechos_conocidos": r["total"],
        })

    # dedupe — same province can appear if our regex is over-broad
    dedup = {}
    for m in matched:
        dedup[m["province_es"]] = m
    matched = list(dedup.values())
    print(f"[match] {len(matched)} provinces", file=sys.stderr)

    missing = [es for (_ru, es, *_rest) in PROVINCES if not any(x["province_es"] == es for x in matched)]
    if missing:
        print(f"[warn] missing: {missing}", file=sys.stderr)

    values = sorted(m["hechos_conocidos"] for m in matched)
    p5 = values[max(0, int(len(values) * 0.05) - 1)]
    p95 = values[min(len(values) - 1, int(len(values) * 0.95))]

    def intensity(v):
        if p95 == p5:
            return 0.5
        return max(0.0, min(1.0, (v - p5) / (p95 - p5)))

    points = [
        [m["lat"], m["lng"], round(intensity(m["hechos_conocidos"]), 3), m["hechos_conocidos"]]
        for m in matched
    ]

    payload = {
        "source": "Informe delitos de odio 2024 — Ministerio del Interior",
        "source_url": PDF_URL,
        "period": "2024",
        "metric": "infracciones penales e incidentes de odio (hechos conocidos)",
        "granularity": "provincia",
        "count": len(points),
        "legend_min": f"{p5} случаев",
        "legend_max": f"{p95}+ случаев",
        "legend_sub": f"Мин-во внутренних дел · 2024 · {len(matched)} провинций",
        "render_radius": 55,
        "render_blur": 45,
        "stats": {
            "min_count": min(values),
            "max_count": max(values),
            "p5": p5,
            "p95": p95,
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
