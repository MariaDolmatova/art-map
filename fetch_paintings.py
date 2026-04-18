"""
Step 1: Filter European Paintings from the Met Open Access CSV, store in DuckDB,
geocode place of origin with Nominatim.

CSV source: https://github.com/metmuseum/openaccess
~260 MB download; saved locally as MetObjects.csv for reuse.
"""

import csv
import os
import time
import duckdb
import requests

MET_CSV_URL = (
    "https://media.githubusercontent.com/media/metmuseum/openaccess/master/MetObjects.csv"
)
NOMINATIM_BASE = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "ArtMapApp/1.0 (maria.tathue@gmail.com)"}

CSV_PATH = "MetObjects.csv"
DB_PATH = "artmap.duckdb"
TARGET_DEPT = "European Paintings"
TEST_BATCH = 200

# Maps Met nationality strings → geocodable place names.
# The first token before a comma is used as the lookup key.
NATIONALITY_TO_PLACE = {
    "French":        "France",
    "Italian":       "Italy",
    "Dutch":         "Netherlands",
    "British":       "United Kingdom",
    "German":        "Germany",
    "Netherlandish": "Netherlands",
    "Flemish":       "Belgium",
    "Spanish":       "Spain",
    "Swiss":         "Switzerland",
    "Norwegian":     "Norway",
    "Russian":       "Russia",
    "Danish":        "Denmark",
    "Swedish":       "Sweden",
    "Greek":         "Greece",
    "Belgian":       "Belgium",
    "Austrian":      "Austria",
    "Irish":         "Ireland",
    "Hungarian":     "Hungary",
    "Polish":        "Poland",
    "Portuguese":    "Portugal",
    "Romanian":      "Romania",
    "Czech":         "Czech Republic",
    "Finnish":       "Finland",
    "Scottish":      "Scotland, United Kingdom",
    "Armenian":      "Armenia",
}


# ── Download ────────────────────────────────────────────────────────────────

def download_csv():
    print(f"Downloading Met Open Access CSV (~260 MB) → {CSV_PATH}")
    print("This only happens once; subsequent runs use the local file.\n")
    with requests.get(MET_CSV_URL, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(CSV_PATH, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    print(f"\r  {downloaded / 1e6:.1f} MB / {total / 1e6:.1f} MB  ({pct:.0f}%)", end="", flush=True)
    print(f"\nDownload complete: {CSV_PATH}\n")


# ── Parse ───────────────────────────────────────────────────────────────────

def build_place(row: dict) -> tuple[str, str] | None:
    """Return (place_of_origin, geocode_query) or None if no location data.

    For European Paintings the geographic columns are empty; we derive place
    from Artist Nationality instead (e.g. 'French' → 'France').
    """
    # Try geographic columns first (useful when adding other departments later)
    city    = row.get("City", "").strip()
    country = row.get("Country", "").strip()
    region  = row.get("Region", "").strip()
    if city and country:
        return f"{city}, {country}", f"{city}, {country}"
    if country:
        return country, country
    if region:
        return region, region

    # Fall back to Artist Nationality
    nat = row.get("Artist Nationality", "").strip()
    if not nat:
        return None
    # Take the first token before any comma/pipe (handles "British, Scottish" etc.)
    primary = nat.split(",")[0].split("|")[0].strip()
    place = NATIONALITY_TO_PLACE.get(primary, primary)  # map or pass through as-is
    return nat, place  # store raw nationality, geocode the mapped place


def parse_csv(path: str, target: int) -> list[dict]:
    records = []
    scanned = 0

    with open(path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("Department", "").strip() != TARGET_DEPT:
                continue
            scanned += 1
            result = build_place(row)
            if result is None:
                continue
            place_of_origin, geocode_query = result
            records.append({
                "object_id":      row.get("Object ID", "").strip(),
                "title":          row.get("Title", "").strip(),
                "artist":         row.get("Artist Display Name", "").strip(),
                "date":           row.get("Object Date", "").strip(),
                "culture":        row.get("Culture", "").strip(),
                "place_of_origin": place_of_origin,
                "geocode_query":  geocode_query,
                "country":        row.get("Country", "").strip(),
                "city":           row.get("City", "").strip(),
                "region":         row.get("Region", "").strip(),
                "medium":         row.get("Medium", "").strip(),
                "period":         row.get("Period", "").strip(),
                "classification": row.get("Classification", "").strip(),
                "thumbnail":      "",  # not in CSV; link to Met page instead
                "museum":         "Metropolitan Museum of Art",
                "museum_url":     row.get("Link Resource", "").strip(),
            })
            if len(records) % 50 == 0:
                print(f"  {len(records):>3} collected (scanned {scanned} European Paintings rows)", flush=True)
            if len(records) >= target:
                break

    print(f"\nParsed {scanned} European Paintings rows → kept {len(records)} with place of origin.")
    return records


# ── Database ────────────────────────────────────────────────────────────────

def init_db(con: duckdb.DuckDBPyConnection):
    con.execute("""
        CREATE OR REPLACE TABLE paintings (
            object_id       VARCHAR PRIMARY KEY,
            title           VARCHAR,
            artist          VARCHAR,
            date            VARCHAR,
            culture         VARCHAR,
            place_of_origin VARCHAR,
            geocode_query   VARCHAR,
            country         VARCHAR,
            city            VARCHAR,
            region          VARCHAR,
            medium          VARCHAR,
            period          VARCHAR,
            classification  VARCHAR,
            thumbnail       VARCHAR,
            museum          VARCHAR,
            museum_url      VARCHAR,
            lat             DOUBLE,
            lng             DOUBLE
        )
    """)


# ── Geocoding ───────────────────────────────────────────────────────────────

def geocode(query: str) -> tuple[float | None, float | None]:
    try:
        resp = requests.get(
            NOMINATIM_BASE,
            params={"q": query, "format": "json", "limit": 1},
            headers=NOMINATIM_HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        results = resp.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        print(f"    error: {e}")
    return None, None


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    if not os.path.exists(CSV_PATH):
        download_csv()
    else:
        size_mb = os.path.getsize(CSV_PATH) / 1e6
        print(f"Using cached {CSV_PATH} ({size_mb:.0f} MB)\n")

    print(f"Scanning CSV for European Paintings with place of origin (target: {TEST_BATCH})...")
    records = parse_csv(CSV_PATH, TEST_BATCH)

    # Save to DuckDB
    con = duckdb.connect(DB_PATH)
    init_db(con)

    for r in records:
        con.execute("""
            INSERT OR REPLACE INTO paintings VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL
            )
        """, [
            r["object_id"], r["title"], r["artist"], r["date"],
            r["culture"], r["place_of_origin"], r["geocode_query"],
            r["country"], r["city"], r["region"],
            r["medium"], r["period"], r["classification"],
            r["thumbnail"], r["museum"], r["museum_url"],
        ])
    print(f"Saved {len(records)} rows to {DB_PATH}.\n")

    # Geocode unique places
    unique = con.execute(
        "SELECT DISTINCT geocode_query FROM paintings WHERE lat IS NULL"
    ).fetchall()
    print(f"Geocoding {len(unique)} unique places (Nominatim, 1 req/s)...")

    geocache: dict[str, tuple] = {}
    for i, (query,) in enumerate(unique, 1):
        print(f"  [{i:>3}/{len(unique)}] {query}", flush=True)
        lat, lng = geocode(query)
        geocache[query] = (lat, lng)
        if lat is None:
            print(f"    ⚠  no result")
        time.sleep(1.1)

    for query, (lat, lng) in geocache.items():
        con.execute(
            "UPDATE paintings SET lat = ?, lng = ? WHERE geocode_query = ?",
            [lat, lng, query],
        )

    # Summary
    total    = con.execute("SELECT COUNT(*) FROM paintings").fetchone()[0]
    geocoded = con.execute("SELECT COUNT(*) FROM paintings WHERE lat IS NOT NULL").fetchone()[0]
    failed   = total - geocoded

    print(f"\n{'─'*60}")
    print(f"  Total saved :  {total}")
    print(f"  Geocoded    :  {geocoded}  ({geocoded/total*100:.1f}%)" if total else "  No rows saved.")
    print(f"  Failed/null :  {failed}")
    print(f"  Database    :  {DB_PATH}")
    print(f"{'─'*60}")

    print("\nSample rows:")
    rows = con.execute("""
        SELECT title, artist, place_of_origin, lat, lng
        FROM paintings WHERE lat IS NOT NULL LIMIT 8
    """).fetchall()
    for title, artist, place, lat, lng in rows:
        print(f"  {title[:50]:<50} | {artist[:30]:<30} | {place:<45} | {lat:.2f},{lng:.2f}")

    if failed:
        print("\nFailed geocodes:")
        for (q,) in con.execute(
            "SELECT DISTINCT geocode_query FROM paintings WHERE lat IS NULL"
        ).fetchall():
            print(f"  ✗ {q}")

    con.close()


if __name__ == "__main__":
    main()
