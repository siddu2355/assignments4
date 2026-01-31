#!/usr/bin/env python3
"""
gmaps_city_areas_scraper.py

Extend your original gmaps scraper to extract MAJOR areas/subareas/localities for a given city name.

Usage:
  python gmaps_city_areas_scraper.py --city "Ghaziabad, India" -o ghaziabad_areas.csv
  python gmaps_city_areas_scraper.py --city "Ghaziabad" --resolve --top 200 -o out.csv

Notes:
 - This uses Selenium to scrape Google Maps pages (UI is brittle and may change).
 - For production use prefer official sources (Google Places API, India Post PIN dataset, OSM+Overpass).
 - Tune WAIT_AFTER_LOAD, QUERY_THROTTLE, and HEADLESS to fit your environment.
"""

import time
import re
import argparse
import pandas as pd
from urllib.parse import quote_plus
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from collections import defaultdict

# -----------------------------
# CONFIGURATION (edit these)
# -----------------------------
OUTPUT_CSV = "city_areas.csv"
WAIT_AFTER_LOAD = 5      # seconds to wait after loading a maps search (increase if slow)
CLICK_WAIT = 1.2         # wait between clicks and parsing
HEADLESS = False         # set True if you want headless (may increase blocking)
QUERY_THROTTLE = 2       # seconds between queries (avoid rate-limits)
MAX_RESOLVE = 300        # max number of candidates to resolve (lat/lng) — set lower in heavy runs
MIN_SCORE_TO_KEEP = 3    # final filter: keep candidates with score >= this OR count >= 2

# Heuristics for identifying locality names
MAJOR_SUFFIXES = [
    "nagar", "colony", "vihar", "village", "enclave", "sector", "sector-", "sector ",
    "extension", "garden", "puram", "ganj", "bazar", "market", "crossing", "chungi",
    "chowk", "bagh", "park", "block", "phase", "village", "industrial", "industrial area",
    "industrial estate", "colony", "suburb", "town"
]
SUFFIX_REGEX = re.compile(r'\b(' + r'|'.join([re.escape(s) for s in MAJOR_SUFFIXES]) + r')\b', re.I)

# text normalization
def normalize_name(n):
    if not n: return ""
    s = n.strip()
    s = re.sub(r'\s+', ' ', s)
    s = s.strip(" ,.-")
    return s

def create_driver():
    options = webdriver.ChromeOptions()
    if HEADLESS:
        # new headless mode
        options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    # avoid automation flags if desired (careful)
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver

def get_lat_lng_from_url(url):
    m = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', url)
    if m:
        return m.group(1), m.group(2)
    m2 = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', url)
    if m2:
        return m2.group(1), m2.group(2)
    return None, None

# --- Reuse / slightly modified Selenium_extractor from your original ---
def Selenium_extractor(address, driver=None, wait_after_load=WAIT_AFTER_LOAD, click_wait=CLICK_WAIT):
    """
    Given an address string, load Google Maps search and try to extract formatted address,
    lat/lng and components. Returns dict with keys formatted_address, latitude, longitude.
    If driver provided, re-uses it (faster).
    """
    own_driver = False
    if driver is None:
        driver = create_driver()
        own_driver = True

    out = {"formatted_address": None, "latitude": None, "longitude": None}
    try:
        search_url = "https://www.google.com/maps/search/" + quote_plus(address)
        driver.get(search_url)
        time.sleep(wait_after_load)

        # try to get lat/lng from the URL first
        cur_url = driver.current_url
        lat, lng = get_lat_lng_from_url(cur_url)

        # try clicking first place / result element to open details panel
        try:
            # try to find place links
            a_candidates = driver.find_elements(By.XPATH, "//a[contains(@href, '/place/') or contains(@href, '/search/')]")
            if not a_candidates:
                a_candidates = driver.find_elements(By.CLASS_NAME, "hfpxzc")
            if a_candidates:
                elem = a_candidates[0]
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", elem)
                    time.sleep(0.3)
                    driver.execute_script("arguments[0].click();", elem)
                except Exception:
                    try:
                        elem.click()
                    except Exception:
                        pass
                time.sleep(click_wait)
        except Exception:
            pass

        # after clicking, update lat/lng from URL/href
        try:
            cur_url = driver.current_url
            lat2, lng2 = get_lat_lng_from_url(cur_url)
            if lat2 and lng2:
                lat, lng = lat2, lng2
        except Exception:
            pass

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        # get a reasonable human-readable formatted address
        # try common selectors first
        formatted = None
        try:
            # existing heuristic: look for Io6YTe fontBodyMedium kR99db fdkmkc classes
            divs = soup.find_all('div', {"class": "Io6YTe fontBodyMedium kR99db fdkmkc"})
            if divs:
                for d in divs:
                    txt = d.get_text(" ", strip=True)
                    if txt and len(txt) > 5:
                        formatted = txt
                        break
        except Exception:
            pass

        if not formatted:
            # try meta description
            try:
                meta = soup.find('meta', {'property': 'og:description'})
                if meta and meta.get('content'):
                    formatted = meta['content']
            except Exception:
                pass

        # fallback: pick a long comma-containing text line
        if not formatted:
            all_text = soup.get_text(separator="\n", strip=True)
            for line in all_text.splitlines():
                if len(line) > 20 and (',' in line):
                    formatted = line.strip()
                    break

        out['formatted_address'] = formatted
        out['latitude'] = lat
        out['longitude'] = lng
        return out

    except Exception as e:
        print(f"[ERROR] Selenium_extractor('{address}') -> {e}")
        return out
    finally:
        if own_driver:
            driver.quit()

# -----------------------------
# Candidate collection & parsing heuristics
# -----------------------------
def parse_candidates_from_soup(soup, raw_html_text):
    """
    Multiple heuristics to pull locality-like candidate names from a Google Maps page.
    Returns dict: name -> set(of heuristic tags) - to calculate weight later.
    """
    candidates = defaultdict(set)

    # 1) anchors pointing to /place/ or /search/
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '/place/' in href or '/search/' in href or '/maps/place' in href:
            txt = a.get_text(" ", strip=True)
            if txt and len(txt) >= 3 and len(txt) <= 80:
                n = normalize_name(txt)
                if n:
                    candidates[n].add('anchor')

    # 2) aria-labels / role=article (cards)
    for el in soup.find_all(attrs={"aria-label": True}):
        txt = el.get('aria-label').strip()
        if txt and len(txt) >= 3 and len(txt) <= 80:
            n = normalize_name(txt)
            if n:
                candidates[n].add('aria')

    # 3) visible text lines that contain common suffixes (Nagar, Colony, Vihar etc)
    for line in raw_html_text.splitlines():
        line = line.strip()
        if not line or len(line) < 3 or len(line) > 120:
            continue
        # quick punctuation cleanup
        # if suffix appears, capture probable name substring (split by comma/pipe/dash)
        if SUFFIX_REGEX.search(line):
            # try split heuristics:
            parts = re.split(r'[\|\n\r\u2022\-–—]', line)
            for p in parts:
                p = p.strip()
                if SUFFIX_REGEX.search(p) and 3 <= len(p) <= 80:
                    n = normalize_name(p)
                    if n:
                        candidates[n].add('suffix_text')

    # 4) look for map-card style divs: (some pages put names in spans)
    # fallback: any capitalized multiword tokens that look like localities
    page_text = raw_html_text
    tokens = re.findall(r'\b([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,}){0,4})\b', page_text)
    for t in tokens:
        if SUFFIX_REGEX.search(t) or len(t.split()) >= 2:
            n = normalize_name(t)
            if 3 <= len(n) <= 80:
                candidates[n].add('cap_token')

    return candidates  # dict name -> set(str)

def collect_major_area_candidates(city, driver, queries=None, throttle=QUERY_THROTTLE):
    """
    Run several templated queries on Google Maps for a city and build scored candidate list.
    Returns dict: normalized_name -> {name_example, count, heuristics_set, score}
    """
    if queries is None:
        queries = [
            f"neighborhoods in {city}",
            f"localities in {city}",
            f"areas in {city}",
            f"suburbs of {city}",
            f"{city} neighbourhoods",
            f"{city} localities",
            f"{city} colonies",
            f"{city} sectors {city}",   # sometimes sector searches show sector lists
            f"{city} market",
            f"{city} crossing",
        ]

    accumulated = {}  # norm_name -> {examples:set(), count:int, heuristics:set()}
    city_norm = normalize_name(city).lower()

    for q in queries:
        try:
            url = "https://www.google.com/maps/search/" + quote_plus(q)
            driver.get(url)
            time.sleep(WAIT_AFTER_LOAD)
            page = driver.page_source
            soup = BeautifulSoup(page, 'html.parser')
            found = parse_candidates_from_soup(soup, soup.get_text("\n", strip=True))
            # record findings
            for norm_name, heuristics in found.items():
                name_example = norm_name
                if norm_name.lower() == city_norm:
                    continue  # skip region name itself
                entry = accumulated.get(norm_name)
                if not entry:
                    entry = {"examples": set(), "count": 0, "heuristics": set()}
                entry["examples"].add(name_example)
                entry["count"] += 1
                entry["heuristics"].update(heuristics)
                accumulated[norm_name] = entry
        except Exception as e:
            print(f"[WARN] query '{q}' failed: {e}")
        time.sleep(throttle)

    # compute score (weights)
    results = {}
    for name, info in accumulated.items():
        score = 0
        # anchor hits are strong
        if 'anchor' in info["heuristics"]:
            score += 5
        if 'aria' in info["heuristics"]:
            score += 3
        if 'suffix_text' in info["heuristics"]:
            score += 3
        if 'cap_token' in info["heuristics"]:
            score += 1
        # count across different queries increases confidence
        score += info["count"] * 2
        # length/word heuristics
        if len(name.split()) >= 2:
            score += 1
        results[name] = {
            "name_examples": ";".join(sorted(info["examples"]))[:200],
            "count": info["count"],
            "heuristics": ",".join(sorted(info["heuristics"])),
            "score": score
        }
    return results

# -----------------------------
# Main orchestration & CSV output
# -----------------------------
def run_for_city(city, output_csv=OUTPUT_CSV, resolve=False, top_n=200,
                 min_score=MIN_SCORE_TO_KEEP, headless=HEADLESS):
    global HEADLESS
    HEADLESS = headless

    driver = create_driver()
    try:
        print(f"[1/3] Collecting candidate areas (multiple queries) for: {city}")
        candidates = collect_major_area_candidates(city, driver)

        # convert to list and apply basic filtering
        rows = []
        for norm, info in candidates.items():
            if info["score"] < min_score and info["count"] < 2:
                # filter out very low-confidence singletons
                continue
            rows.append({
                "name_norm": norm,
                "name_examples": info["name_examples"],
                "count": info["count"],
                "heuristics": info["heuristics"],
                "score": info["score"]
            })

        if not rows:
            print("No candidates found - try increasing WAIT_AFTER_LOAD or decrease min_score.")
            df_empty = pd.DataFrame([], columns=["name", "name_examples", "score", "count", "heuristics", "formatted_address", "latitude", "longitude"])
            df_empty.to_csv(output_csv, index=False, encoding="utf-8")
            return

        # sort by score, count
        rows = sorted(rows, key=lambda r: (r["score"], r["count"]), reverse=True)
        print(f"Collected {len(rows)} candidates; keeping top {top_n}")

        # optionally resolve to formatted address + lat/lng using Selenium_extractor
        resolved = []
        if resolve:
            # limit resolve count to MAX_RESOLVE or top_n
            limit = min(top_n, MAX_RESOLVE, len(rows))
            print(f"[2/3] Resolving top {limit} candidates to get coordinates (this may be slow)...")
            for i in range(limit):
                candidate = rows[i]
                # pick a good search string: example + city
                search_name = candidate["name_examples"].split(";")[0] + ", " + city
                try:
                    res = Selenium_extractor(search_name, driver=driver)
                except Exception as e:
                    print(f"[WARN] resolve '{search_name}' failed: {e}")
                    res = {"formatted_address": None, "latitude": None, "longitude": None}
                resolved.append({
                    "name": candidate["name_norm"],
                    "name_examples": candidate["name_examples"],
                    "score": candidate["score"],
                    "count": candidate["count"],
                    "heuristics": candidate["heuristics"],
                    "formatted_address": res.get("formatted_address"),
                    "latitude": res.get("latitude"),
                    "longitude": res.get("longitude")
                })
                # small throttle
                time.sleep(1.0)
            # also append remaining top rows without resolving (but with empty coords)
            for j in range(limit, min(top_n, len(rows))):
                candidate = rows[j]
                resolved.append({
                    "name": candidate["name_norm"],
                    "name_examples": candidate["name_examples"],
                    "score": candidate["score"],
                    "count": candidate["count"],
                    "heuristics": candidate["heuristics"],
                    "formatted_address": None,
                    "latitude": None,
                    "longitude": None
                })
        else:
            # no resolve: return top_n rows with no coords
            for i in range(min(top_n, len(rows))):
                candidate = rows[i]
                resolved.append({
                    "name": candidate["name_norm"],
                    "name_examples": candidate["name_examples"],
                    "score": candidate["score"],
                    "count": candidate["count"],
                    "heuristics": candidate["heuristics"],
                    "formatted_address": None,
                    "latitude": None,
                    "longitude": None
                })

        # build DataFrame and write CSV
        df = pd.DataFrame(resolved, columns=[
            "name", "name_examples", "score", "count", "heuristics", "formatted_address", "latitude", "longitude"
        ])
        df.to_csv(output_csv, index=False, encoding="utf-8")
        print(f"[3/3] Saved {len(df)} rows to {output_csv}")
    finally:
        driver.quit()


# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Google Maps for major areas/subareas of a city")
    parser.add_argument("--city", "-c", required=True, help="City or region name (e.g., 'Ghaziabad, India')")
    parser.add_argument("--output", "-o", default=OUTPUT_CSV, help="Output CSV filename")
    parser.add_argument("--resolve", action="store_true", help="Resolve each candidate to formatted address + lat/lng (slower)")
    parser.add_argument("--top", type=int, default=200, help="How many top-scoring candidates to keep")
    parser.add_argument("--min-score", type=int, default=MIN_SCORE_TO_KEEP, help="Minimum heuristic score to keep a candidate")
    parser.add_argument("--headless", action="store_true", help="Run Chrome headless (may be more likely blocked)")
    args = parser.parse_args()

    run_for_city(args.city, output_csv=args.output, resolve=args.resolve, top_n=args.top, min_score=args.min_score, headless=args.headless)
