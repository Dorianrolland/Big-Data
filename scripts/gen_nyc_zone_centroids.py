"""One-shot script: compute NYC TLC taxi zone centroids from the official shapefile
and write tlc_replay/nyc_zone_centroids.json.

Usage:
    python scripts/gen_nyc_zone_centroids.py
"""
from __future__ import annotations

import json
import tempfile
import urllib.request
import zipfile
from pathlib import Path

import duckdb

ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
OUT_PATH = Path(__file__).resolve().parent.parent / "tlc_replay" / "nyc_zone_centroids.json"


def main() -> None:
    tmp_dir = Path(tempfile.gettempdir()) / "nyc_zones_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    zip_path = tmp_dir / "taxi_zones.zip"

    if not zip_path.exists() or zip_path.stat().st_size < 500_000:
        print(f"downloading {ZONES_URL}")
        urllib.request.urlretrieve(ZONES_URL, zip_path)

    print("extracting shapefile")
    extract_dir = tmp_dir / "extracted"
    extract_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)

    shp_candidates = list(extract_dir.rglob("taxi_zones.shp"))
    if not shp_candidates:
        raise FileNotFoundError("taxi_zones.shp not found after extraction")
    shp_path = shp_candidates[0].as_posix()
    print(f"shp: {shp_path}")

    con = duckdb.connect()
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")

    rows = con.execute(
        f"""
        SELECT LocationID,
               ST_Y(ST_Centroid(ST_Transform(geom, 'EPSG:2263', 'EPSG:4326', always_xy:=true))) AS lat,
               ST_X(ST_Centroid(ST_Transform(geom, 'EPSG:2263', 'EPSG:4326', always_xy:=true))) AS lon
        FROM ST_Read('{shp_path}')
        ORDER BY LocationID
        """
    ).fetchall()

    centroids: dict[str, list[float]] = {}
    for r in rows:
        loc_id, lat, lon = r
        centroids[str(int(loc_id))] = [round(float(lat), 6), round(float(lon), 6)]

    print(f"{len(centroids)} zones computed")
    for z in ("1", "132", "138", "161", "186", "263"):
        print(f"  zone {z}: {centroids.get(z)}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(centroids, f, separators=(",", ":"), sort_keys=True)
    print(f"wrote {OUT_PATH} ({OUT_PATH.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
