"""
Export artmap.duckdb -> data/paintings.json for the static frontend.
Output is grouped by artist: each entry is one artist + their paintings list.
Run after fetch_paintings.py / enrich_locations.py.
"""

import json
import os
import re
import duckdb

DB_PATH = "artmap.duckdb"
OUT_PATH = os.path.join("data", "paintings.json")

# Matches trailing parenthetical with a year, "ca.", or "century":
#   "Style of X (ca. 1800-1810)"  |  "Workshop of X (1510)"  |  "X (late 15th century)"
_DATE_IN_ARTIST = re.compile(
    r'\s*\(([^)]*(?:\d{3,4}|ca\.|century)[^)]*)\)\s*$',
    re.IGNORECASE,
)


def clean_artist_date(artist: str, date: str) -> tuple[str, str]:
    """Move a parenthetical date out of the artist field if date is missing."""
    if date:
        return artist, date
    m = _DATE_IN_ARTIST.search(artist)
    if m:
        return artist[:m.start()].strip(), m.group(1).strip()
    return artist, date


def main():
    os.makedirs("data", exist_ok=True)

    con = duckdb.connect(DB_PATH, read_only=True)

    existing_cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'paintings'"
    ).fetchall()}
    citizenship_col = "citizenship" if "citizenship" in existing_cols else "'' AS citizenship"

    rows = con.execute(f"""
        SELECT
            object_id, title, artist, date,
            place_of_origin, medium, period, {citizenship_col},
            thumbnail, museum, museum_url, lat, lng
        FROM paintings
        WHERE lat IS NOT NULL AND lng IS NOT NULL
        ORDER BY artist, title
    """).fetchall()
    con.close()

    cols = [
        "object_id", "title", "artist", "date",
        "place_of_origin", "medium", "period", "citizenship",
        "thumbnail", "museum", "museum_url", "lat", "lng",
    ]

    # Group by artist + coordinates (same artist may appear at same point)
    groups: dict[tuple, dict] = {}
    for row in rows:
        p = dict(zip(cols, row))
        p["artist"], p["date"] = clean_artist_date(p["artist"] or "", p["date"] or "")

        key = (p["artist"], p["lat"], p["lng"])
        if key not in groups:
            groups[key] = {
                "artist":      p["artist"],
                "birth_place": p["place_of_origin"] or "",
                "citizenship": p["citizenship"] or "",
                "movement":    p["period"] or "",
                "lat":         p["lat"],
                "lng":         p["lng"],
                "paintings":   [],
            }
        groups[key]["paintings"].append({
            "object_id":  p["object_id"],
            "title":      p["title"],
            "date":       p["date"],
            "medium":     p["medium"] or "",
            "museum":     p["museum"] or "",
            "museum_url": p["museum_url"] or "",
            "thumbnail":  p["thumbnail"] or "",
        })

    artists = list(groups.values())

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(artists, f, ensure_ascii=False, separators=(",", ":"))

    total_paintings = sum(len(a["paintings"]) for a in artists)
    print(f"Exported {len(artists)} artists / {total_paintings} paintings -> {OUT_PATH}")


if __name__ == "__main__":
    main()
