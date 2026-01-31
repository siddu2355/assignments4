#!/usr/bin/env python3
"""
region_areas_combined.py

Combine OpenStreetMap (Overpass) + India Post PIN data + (optional) Google Places
to build a high-recall list of major/ notable localities for a given region.

Usage examples:
  # OSM only (no keys/files)
  python region_areas_combined.py --region "Ghaziabad, India" -o ghaziabad_osm.csv

  # OSM + local PIN CSV (recommended)
  python region_areas_combined.py --region "Ghaziabad, India" --pincode-csv /path/to/pincode.csv -o ghaziabad_withpin.csv

  # OSM + Google Places (best recall, needs API key + billing)
  python region_areas_combined.py --region "Ghaziabad, India" --google-key YOUR_KEY -o ghaziabad_google.csv

  # All combined, require --google-key and --pincode-csv
  python region_areas_combined.py --region "Ghaziabad, India" --pincode-csv pincode.csv --google-key YOUR_KEY --min-sources 2 -o ghaziabad_major.csv
"""

import argparse
import math
import time
import requests
import pandas as pd
import re
from typing import List, Dict, Optional, Tuple
from urllib.parse import quote_plus

# Constants
NOMINATIM = "https://nominatim.openstreetmap.org/search"
OVERPASS = "https://overpass-api.de/api/interpreter"
POSTAL_API_BASE = "https://api.postalpincode.in"  # public site; best-effort usage
USER_AGENT = "RegionAreasCombined/1.0 (contact: you@yourorg.example)"

REQUEST_TIMEOUT = 60
RETRY_SLEEP = 2
RETRIES = 3

MAJOR_SUFFIX_REGEX = re.compile(
    r"(nagar|colony|vihar|enclave|sector|puram|ganj|garden|extension|village|industrial|bazar|market|chungi|chowk|colony|park|phase|block)",
    re.I
)

def http_get(url, params=None, headers=None, method="GET", data=None):
    headers = headers or {"User-Agent": USER_AGENT}
    for attempt in range(1, RETRIES+1):
        try:
            if method == "GET":
                r = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            else:
                r = requests.post(url, data=data, headers=headers, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 502, 503, 504):
                time.sleep(RETRY_SLEEP * attempt)
                continue
            r.raise_for_status()
        except requests.RequestException as e:
            if attempt == RETRIES:
                raise
            time.sleep(RETRY_SLEEP * attempt)
    return None

# -----------------------
# Helper: Nominatim geocode to get bbox & osm id
# -----------------------
def geocode_region(region: str, countrycodes: Optional[str] = None) -> dict:
    params = {"q": region, "format": "jsonv2", "limit": 5}
    if countrycodes:
        params["countrycodes"] = countrycodes
    r = http_get(NOMINATIM, params=params)
    if not r:
        return {}
    res = r.json()
    time.sleep(1.0)  # be polite
    if not res:
        return {}
    # pick best candidate (prefer relation/admin)
    def score(c):
        s = 0
        if c.get("osm_type") == "relation":
            s += 2
        if c.get("class") == "boundary" and c.get("type") == "administrative":
            s += 2
        s += float(c.get("importance") or 0)
        return s
    best = sorted(res, key=score, reverse=True)[0]
    return best

# -----------------------
# Overpass query builder (extended extras)
# -----------------------
def build_overpass_query(osm_type: str, osm_id: int, place_types: List[str], include_extra: bool = True) -> str:
    """
    builds an Overpass query that returns:
      - administrative relations
      - place=suburb | neighbourhood | ...
      - optional extras: named landuse=residential, boundary=political, name-suffix matches
    """
    if osm_type == "relation":
        id_selector = f"rel({osm_id});"
    elif osm_type == "way":
        id_selector = f"way({osm_id});"
    else:
        raise ValueError("region must be relation or way")

    place_regex = "|".join(sorted(set(place_types)))
    q = f"""
[out:json][timeout:60];
{id_selector}
map_to_area->.a;

// admin boundaries
(
  rel(area.a)["boundary"="administrative"]["admin_level"];
)->.admins;

// places
(
  nwr(area.a)["place"~"^{place_regex}$"];
)->.places;
"""
    if include_extra:
        suffix_regex = r"(?i)(nagar|colony|vihar|enclave|sector|puram|ganj|garden|extension|village|industrial|bazar|market|chungi|chowk|puram|garh|puram)$"
        q += f"""
// extras: named residential polygons, political boundaries, and name-suffix matches
(
  nwr(area.a)["landuse"="residential"]["name"];
  nwr(area.a)["boundary"="political"]["name"];
  nwr(area.a)["name"~"{suffix_regex}"];
)->.extras;
"""
    q += """
(.admins; .places; .extras;);
out tags center;
"""
    return q

def overpass_run(query: str) -> dict:
    r = http_get(OVERPASS, method="POST", data={"data": query})
    return r.json() if r else {}

def parse_overpass_elements(elements: List[dict], region_name: str, region_admin_level: Optional[str]) -> List[dict]:
    rows = []
    seen = set()
    for e in elements:
        tags = e.get("tags", {})
        name = tags.get("name")
        if not name:
            continue
        place = tags.get("place")
        boundary = tags.get("boundary")
        admin_level = tags.get("admin_level")
        osm_type = e.get("type")
        osm_id = e.get("id")
        center = e.get("center", {})
        lat = center.get("lat") or e.get("lat")
        lon = center.get("lon") or e.get("lon")
        if not name:
            continue
        # category and kind
        if boundary == "administrative":
            category = "admin_subarea"
            kind = f"admin_level_{admin_level}" if admin_level else "admin"
        elif place:
            category = "place"
            kind = place
        else:
            category = "other"
            kind = tags.get("type") or "named"
        key = (name.strip().lower(), category, kind)
        if key in seen:
            continue
        seen.add(key)
        # skip region exact name
        if name.strip().lower() == (region_name or "").strip().lower():
            continue
        rows.append({
            "name": name,
            "source": "osm",
            "category": category,
            "kind": kind,
            "admin_level": admin_level,
            "lat": lat,
            "lon": lon,
            "osm_type": osm_type,
            "osm_id": osm_id,
            "population": tags.get("population"),
            "wikidata": tags.get("wikidata")
        })
    return rows

# -----------------------
# Postal PIN helpers (local CSV and public API)
# -----------------------
def read_pincode_csv(path: str, bbox: Optional[Tuple[float,float,float,float]] = None) -> List[dict]:
    """
    Read a pincode CSV downloaded from data.gov.in.
    Expected to have columns like 'OfficeName', 'Pincode', 'DistrictName', 'StateName', optionally lat/lon.
    If bbox provided, filter by lat/lon inside bbox (min_lat, max_lat, min_lon, max_lon).
    """
    df = pd.read_csv(path, dtype=str, encoding="utf-8", low_memory=False)
    cols = [c.lower() for c in df.columns]
    # try to find sensible columns
    name_col = None
    lat_col = None
    lon_col = None
    for c in df.columns:
        lc = c.lower()
        if "office" in lc or "name" in lc:
            if not name_col:
                name_col = c
        if "latitude" in lc:
            lat_col = c
        if "longitude" in lc:
            lon_col = c
        if "lat" == lc and not lat_col:
            lat_col = c
        if "lon" == lc and not lon_col:
            lon_col = c
    if not name_col:
        # fallback to first column
        name_col = df.columns[0]

    rows = []
    for _, r in df.iterrows():
        name = str(r.get(name_col, "")).strip()
        lat = None
        lon = None
        try:
            lat = float(r[lat_col]) if lat_col and not pd.isna(r[lat_col]) else None
            lon = float(r[lon_col]) if lon_col and not pd.isna(r[lon_col]) else None
        except Exception:
            lat = lon = None
        if bbox and lat is not None and lon is not None:
            min_lat, max_lat, min_lon, max_lon = bbox
            if not (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
                continue
        rows.append({
            "name": name,
            "source": "pin_csv",
            "pin": r.get("Pincode") if "Pincode" in r else r.get("pincode") if "pincode" in r else None,
            "lat": lat,
            "lon": lon,
            "district": r.get("DistrictName") if "DistrictName" in r else r.get("district") if "district" in r else None,
            "state": r.get("StateName") if "StateName" in r else r.get("state") if "state" in r else None
        })
    return rows

def postal_api_search(query: str) -> List[dict]:
    """
    Query the public postal API (best-effort). Endpoint: /postoffice/<name>
    This returns a list of offices matching the query string.
    """
    url = f"{POSTAL_API_BASE}/postoffice/{quote_plus(query)}"
    r = http_get(url)
    if not r:
        return []
    try:
        data = r.json()
    except Exception:
        return []
    out = []
    # data is an array of results; check common structure
    for d in data:
        if isinstance(d, dict) and d.get("Status") == "Success" and isinstance(d.get("PostOffice"), list):
            for po in d["PostOffice"]:
                out.append({
                    "name": po.get("Name"),
                    "pin": po.get("Pincode"),
                    "district": po.get("District"),
                    "state": po.get("State"),
                    "source": "postal_api"
                })
    return out

# -----------------------
# Google Places grid search (optional)
# -----------------------
def places_nearby_search_point(lat: float, lon: float, radius: int, types: List[str], api_key: str) -> List[dict]:
    results = []
    base = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    for t in types:
        params = {"location": f"{lat},{lon}", "radius": radius, "type": t, "key": api_key}
        r = http_get(base, params=params)
        if not r:
            continue
        data = r.json()
        results.extend(data.get("results", []))
        # pagination
        while data.get("next_page_token"):
            token = data["next_page_token"]
            time.sleep(2.0)  # required wait before using token
            r2 = http_get(base, params={"pagetoken": token, "key": api_key})
            if not r2:
                break
            data = r2.json()
            results.extend(data.get("results", []))
    out = []
    for res in results:
        out.append({
            "name": res.get("name"),
            "place_id": res.get("place_id"),
            "lat": res.get("geometry", {}).get("location", {}).get("lat"),
            "lon": res.get("geometry", {}).get("location", {}).get("lng"),
            "types": ",".join(res.get("types", [])),
            "source": "google"
        })
    return out

def places_grid_search(bbox: Tuple[float,float,float,float], api_key: str, radius_m: int = 2000) -> List[dict]:
    """
    Grid search across the bbox; radius_m is the NearbySearch radius per point.
    This will generate many requests for large cities — be mindful of quota/pricing.
    """
    min_lat, max_lat, min_lon, max_lon = bbox
    # step approx = radius in degrees (latitude)
    radius_km = max(0.5, radius_m / 1000.0)
    lat_step = radius_km / 111.0  # approx
    # use average latitude for lon step
    mean_lat = (min_lat + max_lat) / 2.0
    lon_step = radius_km / (111.0 * abs(math.cos(math.radians(mean_lat))) + 1e-6)
    lat = min_lat
    names = []
    types = ["neighborhood", "sublocality"]
    while lat <= max_lat:
        lon = min_lon
        while lon <= max_lon:
            try:
                found = places_nearby_search_point(lat, lon, radius_m, types, api_key)
                names.extend(found)
            except Exception:
                pass
            lon += lon_step * 0.9
            time.sleep(0.2)
        lat += lat_step * 0.9
    # dedupe by place_id or name
    uniq = {}
    for n in names:
        key = n.get("place_id") or (n.get("name") or "").strip().lower()
        if key not in uniq:
            uniq[key] = n
    return list(uniq.values())

# -----------------------
# Normalization, merge and scoring
# -----------------------
def normalize_name(n: str) -> str:
    if not n:
        return ""
    s = n.lower().strip()
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    # remove common tokens like "ghaziabad", "city" if embedded wrongly
    s = re.sub(r'\b(ghaziabad|city|area|sector)\b', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def merge_sources(candidate_lists: List[List[dict]], min_sources: int = 1, fuzzy_merge: bool = True) -> pd.DataFrame:
    """
    Combine candidate lists (each entry has 'name' and 'source' and optional lat/lon).
    Score by count of distinct sources. Return dataframe.
    """
    # canonical map: normalized_name -> aggregated record
    agg = {}
    for lst in candidate_lists:
        for r in lst:
            name = r.get("name")
            if not name:
                continue
            norm = normalize_name(name)
            if not norm:
                continue
            entry = agg.get(norm, {"names": set(), "sources": set(), "lats": [], "lons": [], "rows": []})
            entry["names"].add(name.strip())
            entry["sources"].add(r.get("source"))
            if r.get("lat"):
                try:
                    entry["lats"].append(float(r.get("lat")))
                except Exception:
                    pass
            if r.get("lon"):
                try:
                    entry["lons"].append(float(r.get("lon")))
                except Exception:
                    pass
            entry["rows"].append(r)
            agg[norm] = entry

    rows = []
    for norm, v in agg.items():
        src_count = len(v["sources"])
        if src_count < min_sources:
            # still include but mark as low confidence (keeps original behavior)
            confidence = "low" if min_sources > 1 else "ok"
            # if min_sources>1 we skip; here we skip altogether
            continue
        avg_lat = sum(v["lats"]) / len(v["lats"]) if v["lats"] else None
        avg_lon = sum(v["lons"]) / len(v["lons"]) if v["lons"] else None
        rows.append({
            "name_norm": norm,
            "name_examples": ";".join(sorted(v["names"])),
            "sources": ",".join(sorted(v["sources"])),
            "source_count": src_count,
            "lat": avg_lat,
            "lon": avg_lon,
            "raw_rows": v["rows"]
        })
    df = pd.DataFrame(rows)
    # sort by source_count desc
    if not df.empty:
        df = df.sort_values(by=["source_count", "name_norm"], ascending=[False, True]).reset_index(drop=True)
    return df

# -----------------------
# Major heuristics (optional)
# -----------------------
def is_major_candidate(entry_row: dict, name_norm: str) -> bool:
    # entry_row is the aggregated row dict built above (has source_count, lat, lon etc.)
    sc = int(entry_row.get("source_count", 0))
    if sc >= 2:
        return True
    # name heuristics
    if MAJOR_SUFFIX_REGEX.search(name_norm):
        return True
    return False

# -----------------------
# Main CLI
# -----------------------
def main():
    parser = argparse.ArgumentParser(description="Combine OSM + PIN + Google to extract major localities for a region")
    parser.add_argument("--region", "-r", required=True)
    parser.add_argument("--countrycodes", "-C", default=None)
    parser.add_argument("--pincode-csv", default=None, help="Optional: path to All-India pincode CSV (data.gov.in).")
    parser.add_argument("--use-postal-api", action="store_true", help="Try the public postalpincode.in API for extra candidates (best-effort).")
    parser.add_argument("--google-key", default=None, help="Optional: Google Places API key (billing enabled).")
    parser.add_argument("--min-sources", type=int, default=1, help="Minimum distinct sources required to keep an item in final list (default 1).")
    parser.add_argument("--major-only", action="store_true", help="If set, keep only items judged 'major' by heuristics (higher precision).")
    parser.add_argument("--output", "-o", default="region_localities_combined.csv")
    args = parser.parse_args()

    # Step 1: geocode
    print("[1/6] Geocoding region:", args.region)
    best = geocode_region(args.region, countrycodes=args.countrycodes)
    if not best:
        print("Could not geocode region.")
        return
    bbox = best.get("boundingbox")  # [south, north, west, east] as strings
    if bbox and len(bbox) >= 4:
        min_lat = float(bbox[0])
        max_lat = float(bbox[1])
        min_lon = float(bbox[2])
        max_lon = float(bbox[3])
        bbox_tuple = (min_lat, max_lat, min_lon, max_lon)
    else:
        # fallback to small bbox near lat/lon
        lat = float(best.get("lat"))
        lon = float(best.get("lon"))
        delta = 0.05
        bbox_tuple = (lat-delta, lat+delta, lon-delta, lon+delta)
    region_name = best.get("display_name") or args.region
    osm_type = best.get("osm_type")
    osm_id = int(best.get("osm_id"))

    # Step 2: Overpass (OSM)
    print("[2/6] Querying Overpass (OSM)...")
    overpass_q = build_overpass_query(osm_type, osm_id, ["suburb","neighbourhood","locality","quarter","district"], include_extra=True)
    ov_data = overpass_run(overpass_q)
    elements = ov_data.get("elements", [])
    osm_rows = parse_overpass_elements(elements, region_name, None)
    print(f"  Overpass returned ~{len(osm_rows)} named items")

    all_candidates = []
    all_candidates.append(osm_rows)

    # Step 3: pincode CSV (optional)
    if args.pincode_csv:
        print("[3/6] Reading PIN CSV and filtering inside bbox...")
        pin_rows = read_pincode_csv(args.pincode_csv, bbox=bbox_tuple)
        # convert to same row shape
        pin_rows_conv = []
        for r in pin_rows:
            pin_rows_conv.append({
                "name": r["name"],
                "source": "pin_csv",
                "lat": r.get("lat"),
                "lon": r.get("lon"),
                "pin": r.get("pin"),
                "district": r.get("district")
            })
        print(f"  PIN CSV found {len(pin_rows_conv)} offices inside bbox")
        all_candidates.append(pin_rows_conv)
    else:
        print("[3/6] No PIN CSV provided (optional). Skipping.")

    # Step 4: postal API (optional)
    postal_rows = []
    if args.use_postal_api:
        print("[4/6] Querying public postal API (best-effort). This may be partial...")
        # try a few queries: region token, primary tokens split by comma
        tokens = [t.strip() for t in args.region.split(",") if t.strip()]
        # try region full, then last token (city)
        tried = set()
        for q in [args.region] + tokens[-2:]:
            if q in tried:
                continue
            tried.add(q)
            try:
                res = postal_api_search(q)
                for r in res:
                    postal_rows.append({**r, "source": "postal_api"})
            except Exception:
                pass
            time.sleep(0.5)
        print(f"  postal_api returned ~{len(postal_rows)} items (may include duplicates)")
        if postal_rows:
            all_candidates.append(postal_rows)
    else:
        print("[4/6] Postal API not requested. Skipping.")

    # Step 5: Google Places grid (optional)
    google_rows = []
    if args.google_key:
        print("[5/6] Running Google Places grid search (can be slow/costly)...")
        try:
            google_rows = places_grid_search(bbox_tuple, api_key=args.google_key, radius_m=2000)
            google_rows_conv = []
            for r in google_rows:
                google_rows_conv.append({
                    "name": r.get("name"),
                    "source": "google",
                    "lat": r.get("lat"),
                    "lon": r.get("lon"),
                    "types": r.get("types")
                })
            print(f"  Google Places found {len(google_rows_conv)} unique predictions")
            if google_rows_conv:
                all_candidates.append(google_rows_conv)
        except Exception as e:
            print("  Google Places error:", e)
    else:
        print("[5/6] No Google key supplied. Skipping Google Places.")

    # Step 6: Merge/dedupe/score
    print("[6/6] Merging results and scoring...")
    merged_df = merge_sources(all_candidates, min_sources=args.min_sources)
    if merged_df.empty:
        print("No results (empty after merging). Try lowering --min-sources or include extra inputs.")
        return

    # optional: major-only filter
    if args.major_only:
        kept = []
        for _, row in merged_df.iterrows():
            if is_major_candidate(row.to_dict(), str(row["name_norm"])):
                kept.append(row)
        if kept:
            merged_df = pd.DataFrame(kept).reset_index(drop=True)
        else:
            print("Major-only yielded no rows; outputting original merged list.")

    # Final CSV columns
    out_df = merged_df[["name_examples", "sources", "source_count", "lat", "lon"]].rename(
        columns={"name_examples": "name", "sources": "sources_found", "source_count": "num_sources"}
    )
    out_df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"Saved {len(out_df)} rows to {args.output}")
    print("Done. Tip: if you want to raise recall, run with both --pincode-csv and --google-key together.")

if __name__ == "__main__":
    main()
