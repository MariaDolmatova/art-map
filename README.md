# Art Map

An interactive world map of paintings from the Metropolitan Museum of Art, plotted by the artist's place of birth.

## What it does

- Displays ~7,400 paintings as emoji markers on a Leaflet map, clustered by location
- Each marker represents an artist; clicking it shows their works with dates, medium, and links to the Met website
- **Timeline slider** — filter paintings by year (supports dates like `ca. 1863`, `186[3?]`, `15th century`)
- **Style filter** — toggle art movement categories (Medieval, Renaissance, Baroque, Impressionism, etc.)
- **Sidebar panels** — Art movements and Top 10 countries update live as you filter
- Dark green theme, mobile-responsive

## Data pipeline

```
fetch_paintings.py → enrich_locations.py → export_json.py → data/paintings.json
```

### 1. `fetch_paintings.py`
- Downloads the Met Open Access CSV (~318 MB, cached locally as `MetObjects.csv`)
- Filters all rows where `Classification == "Paintings"`
- Derives place of origin from artist nationality (e.g. `"French"` → `"France"`)
- Geocodes unique places via Nominatim (1 req/s rate limit)
- Stores results in `artmap.duckdb`

### 2. `enrich_locations.py`
- Reads Wikidata URLs from the CSV for stored artists
- Batch-queries Wikidata SPARQL for birth city coordinates, art movements, citizenship
- Updates `artmap.duckdb` with richer location data and metadata

### 3. `export_json.py`
- Exports `artmap.duckdb` → `data/paintings.json`
- Groups by artist, normalises place names (historical empires, nationality adjectives, pipe-duplicates)
- Extracts dates from artist bios when the painting date field is missing

## Setup

```bash
pip install duckdb requests
py fetch_paintings.py       # ~2–3 min (geocoding)
py enrich_locations.py      # ~10–20 min (Wikidata SPARQL)
py export_json.py
```

Then open `index.html` in a browser (or serve locally — no build step needed).

## Data source

[Metropolitan Museum of Art Open Access](https://github.com/metmuseum/openaccess) — released under CC0.
