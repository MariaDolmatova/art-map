"""
Export artmap.duckdb -> data/paintings.json for the static frontend.
Grouped by artist: each entry is one artist + their paintings list.
Run after fetch_paintings.py / enrich_locations.py.
"""

import json
import os
import re
import duckdb

DB_PATH = "artmap.duckdb"
OUT_PATH = os.path.join("data", "paintings.json")
 

def extract_date_from_artist(artist_bio: str) -> str | None:
    # grab "ca. " prefix if present
    ca_prefix = "ca. " if "ca." in artist_bio.lower() else ""
    
    # look for a year or year range anywhere in the string
    m = re.search(r'\d{3,4}(?:[–\-]\d{2,4})?', artist_bio)
    if m:
        return ca_prefix + m.group()

    # keep "late 15th century" as-is
    m = re.search(r'\b(?:early|mid|late)?\s*\d{1,2}(?:st|nd|rd|th)?\s*century\b', artist_bio, re.IGNORECASE)
    if m:
        return m.group().strip()

    # keep "early 1500s" as-is
    m = re.search(r'\b(?:early|mid|late)?\s*\d{3,4}s\b', artist_bio, re.IGNORECASE)
    if m:
        return m.group().strip()
    return None

def clean_artist_date(artist: str, date: str, artist_bio: str) -> tuple[str, str]:
    if date:
        return artist, date
    extracted = extract_date_from_artist(artist_bio)
    return artist, extracted or ""

def main():
    os.makedirs("data", exist_ok=True)

    con = duckdb.connect(DB_PATH, read_only=True)

    existing_cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'paintings'"
    ).fetchall()}

    def col(name):
        return name if name in existing_cols else f"'' AS {name}"

    rows = con.execute(f"""
        SELECT
            object_id, title, artist, date,
            place_of_origin, medium, period,
            {col('citizenship')}, {col('birth_year')}, {col('death_year')},
            thumbnail, museum, museum_url, lat, lng,
            {col('artist_bio')}
        FROM paintings
        WHERE lat IS NOT NULL AND lng IS NOT NULL
        ORDER BY artist, title
    """).fetchall()
    con.close()

    cols = [
        "object_id", "title", "artist", "date",
        "place_of_origin", "medium", "period",
        "citizenship", "birth_year", "death_year",
        "thumbnail", "museum", "museum_url", "lat", "lng",
        "artist_bio",
    ]

    groups: dict[tuple, dict] = {}
    for row in rows:
        p = dict(zip(cols, row))
        p["artist"], p["date"] = clean_artist_date(p["artist"] or "", p["date"] or "", p["artist_bio"] or "")

        key = (p["artist"], p["lat"], p["lng"])
        if key not in groups:
            groups[key] = {
                "artist":      p["artist"],
                "birth_year":  p["birth_year"] or "",
                "death_year":  p["death_year"] or "",
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
