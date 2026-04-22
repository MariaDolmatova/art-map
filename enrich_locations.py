"""
enrich_locations.py: Replace country-level coordinates with artist birth city
coordinates sourced from Wikidata (property P19 = place of birth, P625 = coords).

Requires MetObjects.csv (already downloaded) and artmap.duckdb.
Run after fetch_paintings.py, then re-run export_json.py.
"""

import csv
import re
import time
import duckdb
import requests

DB_PATH  = "artmap.duckdb"
CSV_PATH = "MetObjects.csv"

SPARQL_URL = "https://query.wikidata.org/sparql"
SPARQL_HEADERS = {
    "User-Agent": "ArtMapApp/1.0 (maria.tathue@gmail.com)",
    "Accept": "application/sparql-results+json",
}

BATCH_SIZE = 50  # Q-IDs per SPARQL request


# -- Step 1: collect Wikidata URLs from CSV for our stored paintings ----------

def get_stored_ids(con: duckdb.DuckDBPyConnection) -> set[str]:
    return {str(r[0]) for r in con.execute("SELECT object_id FROM paintings").fetchall()}


def get_wikidata_urls(object_ids: set[str]) -> dict[str, str]:
    """Returns {object_id: wikidata_url} for paintings that have one."""
    mapping = {}
    print(f"Scanning CSV for Wikidata URLs ({len(object_ids)} paintings)...")
    with open(CSV_PATH, encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            oid = row.get("Object ID", "").strip()
            if oid not in object_ids:
                continue
            url = row.get("Artist Wikidata URL", "").strip()
            if url:
                mapping[oid] = url
    print(f"Found Wikidata URLs for {len(mapping)} of {len(object_ids)} paintings.\n")
    return mapping


def extract_qid(url: str) -> str | None:
    m = re.search(r"(Q\d+)", url)
    return m.group(1) if m else None


# -- Step 2: batch-query Wikidata SPARQL -------------------------------------

def query_birth_places(qids: list[str]) -> dict[str, tuple]:
    """
    Returns {qid: (lat, lng, "City, Country", "movement1; movement2", "citizenship")}
    P19 = place of birth, P17 = country (historical), P135 = movement, P27 = citizenship.
    """
    values = " ".join(f"wd:{q}" for q in qids)
    sparql = f"""
    SELECT ?item ?birthPlaceLabel ?countryLabel ?movementLabel ?citizenshipLabel ?birthDate ?deathDate ?lat ?lng WHERE {{
      VALUES ?item {{ {values} }}
      OPTIONAL {{
        ?item wdt:P19 ?birthPlace .
        ?birthPlace wdt:P625 ?coords .
        BIND(geof:latitude(?coords)  AS ?lat)
        BIND(geof:longitude(?coords) AS ?lng)
        OPTIONAL {{ ?birthPlace wdt:P17 ?country . }}
      }}
      OPTIONAL {{ ?item wdt:P135 ?movement . }}
      OPTIONAL {{ ?item wdt:P27  ?citizenship . }}
      OPTIONAL {{ ?item wdt:P569 ?birthDate . }}
      OPTIONAL {{ ?item wdt:P570 ?deathDate . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """
    resp = requests.get(
        SPARQL_URL,
        params={"query": sparql, "format": "json"},
        headers=SPARQL_HEADERS,
        timeout=40,
    )
    resp.raise_for_status()

    movements:    dict[str, list[str]] = {}
    citizenships: dict[str, list[str]] = {}
    coords_data:  dict[str, tuple]     = {}
    birth_years:  dict[str, str]       = {}
    death_years:  dict[str, str]       = {}

    for b in resp.json()["results"]["bindings"]:
        qid = b["item"]["value"].split("/")[-1]

        if "lat" in b and qid not in coords_data:
            city    = b.get("birthPlaceLabel", {}).get("value", "")
            country = b.get("countryLabel",    {}).get("value", "")
            # Drop raw QIDs — Wikidata returns them when no English label exists
            if re.match(r'^Q\d+$', city):    city = ""
            if re.match(r'^Q\d+$', country): country = ""
            label = f"{city}, {country}" if (city and country) else (city or country)
            coords_data[qid] = (float(b["lat"]["value"]), float(b["lng"]["value"]), label)

        mv = b.get("movementLabel",    {}).get("value", "")
        cz = b.get("citizenshipLabel", {}).get("value", "")
        if re.match(r'^Q\d+$', mv): mv = ""
        if re.match(r'^Q\d+$', cz): cz = ""
        if mv and mv not in movements.get(qid, []):
            movements.setdefault(qid, []).append(mv)
        if cz and cz not in citizenships.get(qid, []):
            citizenships.setdefault(qid, []).append(cz)

        # Extract year only (Wikidata dates look like "1606-07-15T00:00:00Z")
        if "birthDate" in b and qid not in birth_years:
            birth_years[qid] = b["birthDate"]["value"][:4]
        if "deathDate" in b and qid not in death_years:
            death_years[qid] = b["deathDate"]["value"][:4]

    results = {}
    for qid, (lat, lng, label) in coords_data.items():
        results[qid] = (
            lat, lng, label,
            "; ".join(movements.get(qid, [])),
            "; ".join(citizenships.get(qid, [])),
            birth_years.get(qid, ""),
            death_years.get(qid, ""),
        )
    return results


def fetch_all_birth_places(qids: list[str]) -> dict[str, tuple]:
    all_results = {}
    total_batches = (len(qids) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(0, len(qids), BATCH_SIZE):
        batch = qids[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        print(f"  Querying Wikidata batch {batch_num}/{total_batches} ({len(batch)} artists)...", flush=True)
        try:
            results = query_birth_places(batch)
            all_results.update(results)
            print(f"    Got birth coords for {len(results)}/{len(batch)}", flush=True)
        except Exception as e:
            print(f"    Batch failed: {e}")
        time.sleep(1.5)  # be polite to Wikidata
    return all_results


# -- Step 3: update DB --------------------------------------------------------

def main():
    con = duckdb.connect(DB_PATH)

    # Add birth_place column if it doesn't exist yet
    cols = {r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'paintings'"
    ).fetchall()}
    if "birth_place" not in cols:
        con.execute("ALTER TABLE paintings ADD COLUMN birth_place VARCHAR")
    if "citizenship" not in cols:
        con.execute("ALTER TABLE paintings ADD COLUMN citizenship VARCHAR")
    if "birth_year" not in cols:
        con.execute("ALTER TABLE paintings ADD COLUMN birth_year VARCHAR")
    if "death_year" not in cols:
        con.execute("ALTER TABLE paintings ADD COLUMN death_year VARCHAR")

    object_ids = get_stored_ids(con)
    oid_to_url = get_wikidata_urls(object_ids)

    if not oid_to_url:
        print("No Wikidata URLs found — nothing to enrich.")
        con.close()
        return

    # Build qid -> [object_ids] mapping (multiple paintings can share an artist)
    qid_to_oids: dict[str, list[str]] = {}
    for oid, url in oid_to_url.items():
        qid = extract_qid(url)
        if qid:
            qid_to_oids.setdefault(qid, []).append(oid)

    unique_qids = list(qid_to_oids.keys())
    print(f"Unique artists to look up: {len(unique_qids)}\n")

    print("Fetching birth places from Wikidata...")
    birth_data = fetch_all_birth_places(unique_qids)

    # Update DB
    updated = 0
    for qid, oids in qid_to_oids.items():
        if qid not in birth_data:
            continue
        lat, lng, place_label, movement, citizenship, birth_year, death_year = birth_data[qid]
        for oid in oids:
            con.execute("""
                UPDATE paintings
                SET lat = ?, lng = ?, birth_place = ?, place_of_origin = ?,
                    period = CASE WHEN (period IS NULL OR period = '') AND ? != '' THEN ? ELSE period END,
                    citizenship = ?, birth_year = ?, death_year = ?
                WHERE object_id = ?
            """, [lat, lng, place_label, place_label, movement, movement,
                  citizenship, birth_year, death_year, oid])
            updated += 1

    total    = con.execute("SELECT COUNT(*) FROM paintings").fetchone()[0]
    enriched = con.execute("SELECT COUNT(*) FROM paintings WHERE birth_place IS NOT NULL AND birth_place != ''").fetchone()[0]
    no_data  = total - updated

    print(f"\n{'-'*60}")
    print(f"  Total paintings  : {total}")
    print(f"  Updated with birth city : {updated}")
    print(f"  No Wikidata birth data  : {no_data}")
    print(f"{'-'*60}")

    print("\nSample enriched rows:")
    rows = con.execute("""
        SELECT title, artist, birth_place, lat, lng
        FROM paintings
        WHERE birth_place IS NOT NULL AND birth_place != ''
        LIMIT 8
    """).fetchall()
    for title, artist, place, lat, lng in rows:
        print(f"  {title[:38]:<38} | {artist[:22]:<22} | {place:<25} | {lat:.3f},{lng:.3f}")

    con.close()
    print("\nDone. Run export_json.py to update the frontend data.")


if __name__ == "__main__":
    main()
