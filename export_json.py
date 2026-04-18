"""
Export artmap.duckdb → data/paintings.json for the static frontend.
Run this after fetch_paintings.py whenever the database changes.
"""

import json
import os
import duckdb

DB_PATH = "artmap.duckdb"
OUT_PATH = os.path.join("data", "paintings.json")


def main():
    os.makedirs("data", exist_ok=True)

    con = duckdb.connect(DB_PATH, read_only=True)
    rows = con.execute("""
        SELECT
            object_id, title, artist, date,
            place_of_origin, country, medium, period,
            classification, thumbnail, museum, museum_url,
            lat, lng
        FROM paintings
        WHERE lat IS NOT NULL AND lng IS NOT NULL
        ORDER BY title
    """).fetchall()
    con.close()

    cols = [
        "object_id", "title", "artist", "date",
        "place_of_origin", "country", "medium", "period",
        "classification", "thumbnail", "museum", "museum_url",
        "lat", "lng",
    ]
    paintings = [dict(zip(cols, row)) for row in rows]

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(paintings, f, ensure_ascii=False, separators=(",", ":"))

    print(f"Exported {len(paintings)} paintings -> {OUT_PATH}")


if __name__ == "__main__":
    main()
