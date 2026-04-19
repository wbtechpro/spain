"""
Fetch Ookla Open Data (fixed broadband) for Spain and aggregate into a heatmap-ready JSON.

Source: https://registry.opendata.aws/speedtest-global-performance/
License: CC BY-NC-SA 4.0 (Ookla Open Data) — attribution required.

Output: data/internet_speed.json
Format: {
  "source": "Ookla Open Data",
  "quarter": "2025-Q4",
  "cell_size_deg": 0.1,
  "mbps_min": <float>,
  "mbps_max": <float>,
  "points": [[lat, lng, intensity_0_1, avg_mbps], ...]
}
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = ROOT / "data" / "internet_speed.json"

# Iberian peninsula + Balearics + Canaries
SPAIN_BBOXES = [
    # (south, west, north, east)
    (35.9, -9.5, 43.9, 4.5),      # Mainland + Balearics
    (27.5, -18.5, 29.5, -13.0),   # Canary Islands
]

# Cell size for spatial aggregation (degrees). ~11 km at Spain's latitude.
CELL_DEG = 0.1

# Which Ookla quarter to fetch. Ookla publishes with a ~6 week lag.
QUARTERS = [
    ("2025", "4", "2025-10-01"),
    ("2025", "3", "2025-07-01"),
    ("2025", "2", "2025-04-01"),
]


def url_for(year: str, quarter: str, date: str) -> str:
    return (
        f"https://ookla-open-data.s3.amazonaws.com/parquet/performance/"
        f"type=fixed/year={year}/quarter={quarter}/{date}_performance_fixed_tiles.parquet"
    )


def fetch(con: duckdb.DuckDBPyConnection, year: str, quarter: str, date: str):
    url = url_for(year, quarter, date)
    print(f"[fetch] Trying Ookla fixed broadband tiles: {url}", file=sys.stderr)

    bbox_conditions = " OR ".join(
        f"(ST_X(c) BETWEEN {w} AND {e} AND ST_Y(c) BETWEEN {s} AND {n})"
        for (s, w, n, e) in SPAIN_BBOXES
    )

    q = f"""
    WITH src AS (
        SELECT
            avg_d_kbps, avg_u_kbps, avg_lat_ms, tests,
            ST_Centroid(ST_GeomFromText(tile)) AS c
        FROM read_parquet('{url}')
    ),
    spain AS (
        SELECT
            ST_Y(c) AS lat,
            ST_X(c) AS lng,
            avg_d_kbps, avg_u_kbps, tests
        FROM src
        WHERE {bbox_conditions}
    ),
    aggregated AS (
        SELECT
            ROUND(lat / {CELL_DEG}) * {CELL_DEG} AS lat_bin,
            ROUND(lng / {CELL_DEG}) * {CELL_DEG} AS lng_bin,
            SUM(avg_d_kbps * tests)::DOUBLE / NULLIF(SUM(tests), 0) / 1000.0 AS avg_d_mbps,
            SUM(tests) AS total_tests
        FROM spain
        GROUP BY lat_bin, lng_bin
        HAVING SUM(tests) >= 3
    )
    SELECT lat_bin, lng_bin, avg_d_mbps, total_tests
    FROM aggregated
    ORDER BY avg_d_mbps DESC
    """
    return con.execute(q).fetchall()


def main() -> int:
    print("[setup] Opening DuckDB, loading httpfs + spatial…", file=sys.stderr)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("SET s3_region='us-east-1';")

    rows = None
    quarter_used = None
    for (year, quarter, date) in QUARTERS:
        try:
            rows = fetch(con, year, quarter, date)
            quarter_used = f"{year}-Q{quarter}"
            break
        except Exception as e:
            print(f"[warn] Quarter {year}-Q{quarter} failed: {e}", file=sys.stderr)
            continue

    if not rows:
        print("[error] No Ookla quarter available. Bailing.", file=sys.stderr)
        return 1

    print(f"[ok] Got {len(rows)} aggregated cells for {quarter_used}", file=sys.stderr)

    mbps_values = [r[2] for r in rows if r[2] is not None]
    mbps_min = min(mbps_values)
    mbps_max = max(mbps_values)

    # Use 5th and 95th percentiles for normalization so outliers don't crush the gradient
    mbps_sorted = sorted(mbps_values)
    p5 = mbps_sorted[int(len(mbps_sorted) * 0.05)]
    p95 = mbps_sorted[int(len(mbps_sorted) * 0.95)]

    def normalize(v: float) -> float:
        if p95 == p5:
            return 0.5
        return max(0.0, min(1.0, (v - p5) / (p95 - p5)))

    points = [
        [round(lat, 3), round(lng, 3), round(normalize(mbps), 3), round(mbps, 1)]
        for (lat, lng, mbps, _tests) in rows
        if mbps is not None
    ]

    payload = {
        "source": "Ookla Open Data",
        "source_url": "https://registry.opendata.aws/speedtest-global-performance/",
        "license": "CC BY-NC-SA 4.0",
        "period": quarter_used,
        "metric": "avg_download_mbps (fixed broadband)",
        "cell_size_deg": CELL_DEG,
        "count": len(points),
        "legend_min": f"{round(p5)} Мбит/с",
        "legend_max": f"{round(p95)} Мбит/с",
        "legend_sub": f"Ookla · {quarter_used} · {len(points)} ячеек (10 км)",
        "render_radius": 25,
        "render_blur": 20,
        "stats": {
            "mbps_min": round(mbps_min, 1),
            "mbps_max": round(mbps_max, 1),
            "mbps_p5": round(p5, 1),
            "mbps_p95": round(p95, 1),
        },
        "points": points,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False))
    size_kb = OUT_PATH.stat().st_size / 1024
    print(f"[ok] Wrote {OUT_PATH} ({size_kb:.1f} KB, {len(points)} cells)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
