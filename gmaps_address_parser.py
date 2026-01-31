# gmaps_address_parser.py
# Requires: selenium, beautifulsoup4, pandas
# Make sure chromedriver is on PATH and Chrome version matches.
# Usage: edit INPUT_CSV, OUTPUT_CSV, INPUT_COL as needed, then run.

import time
import re
import argparse
import pandas as pd
from urllib.parse import quote_plus
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver import ActionChains as AC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# -----------------------------
# CONFIGURATION (edit these)
# -----------------------------
INPUT_CSV = "entity_format.csv"      # path to input CSV
OUTPUT_CSV = "output_addresses.csv"    # path to output CSV
INPUT_COL = "address"                  # name of column containing addresses in input CSV

# The return fields list (modify this list to change which columns are returned)
RETURN_FIELDS = [
    "formatted_address",
    "pin",
    "area",
    "street",
    "locality",
    "city",
    "state",
    "latitude",
    "longitude"
]

# Selenium settings
WAIT_AFTER_LOAD = 6      # seconds to wait after loading a maps search (increase if slow)
CLICK_WAIT = 2           # wait between clicks and parsing
HEADLESS = False         # set True if you want headless (may be more likely to be blocked)
MAX_RETRIES = 2          # retries per address on transient failures
USER_AGENT = None        # optionally set a custom user-agent string

# throttle between queries to reduce bot detection
QUERY_THROTTLE = 3  # seconds

# -----------------------------
# Helper utilities
# -----------------------------
def create_driver():
    options = webdriver.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new") #un Chrome without opening a visible window.
    options.add_argument("--disable-blink-features=AutomationControlled") #helps avoid detection as a bot.
    options.add_argument("--disable-dev-shm-usage") #avoids memory-related issues in Docker/Linux.
    options.add_argument("--no-sandbox") #required in some restricted environments.
    if USER_AGENT:
        options.add_argument(f"--user-agent={USER_AGENT}") #makes the browser pretend it’s a specific device/browser.
    # more options can be added if needed
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver

def get_lat_lng_from_url(url):
    # try patterns: @lat,lng or !3dlat!4dlng or /data=!3m1!4b1!4m5!3m4! etc
    m = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', url)
    if m:
        return m.group(1), m.group(2)
    m2 = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', url)
    if m2:
        return m2.group(1), m2.group(2)
    return None, None

def find_formatted_address_from_soup(soup, page_source):
    """
    Try several heuristics / selectors to obtain a human-readable formatted address string.
    Returns string or None.
    """
    # 1) Common Google Maps panel address selector used in many versions:
    try:
        # your earlier code used: Io6YTe fontBodyMedium kR99db fdkmkc
        divs = soup.find_all('div', {"class": "Io6YTe fontBodyMedium kR99db fdkmkc"})
        if divs:
            # The first such div is often the address line
            for d in divs:
                txt = d.get_text(strip=True)
                if txt and len(txt) > 5 and re.search(r'\d{4,6}', txt):
                    return txt
            # fallback to first match anyway
            return divs[0].get_text(" ", strip=True)
    except Exception:
        pass

    # 2) meta property (sometimes present)
    try:
        meta = soup.find('meta', {'property': 'og:description'})
        if meta and meta.get('content'):
            return meta['content']
    except Exception:
        pass

    # 3) look for any long text snippet containing a postal code (India 6-digit) or common separators
    pin_match = re.search(r'\b\d{6}\b', page_source)
    if pin_match:
        # expand left and right to nearest commas/newlines to get a candidate substring
        idx = pin_match.start()
        start = page_source.rfind(',', 0, idx)
        if start == -1:
            start = max(0, idx - 120)
        end = page_source.find(',', idx)
        if end == -1:
            end = idx + 120
        candidate = page_source[start+1:end].strip()
        # clean HTML tags
        candidate_text = re.sub(r'<[^>]+>', ' ', candidate)
        candidate_text = re.sub(r'\s+', ' ', candidate_text).strip()
        if len(candidate_text) > 5:
            return candidate_text

    # 4) fallback: find any text nodes that look like addresses (comma-separated words)
    all_text = soup.get_text(separator="|", strip=True)
    for piece in all_text.split("|"):
        if len(piece) > 20 and (',' in piece) and (re.search(r'\d{4,6}', piece) or len(piece.split(',')) >= 3):
            return piece.strip()

    return None

def parse_address_components(formatted_address):
    """
    Best-effort heuristics to split a formatted address into pin, area, street, locality, city, state.
    This is heuristic-based because different regions have different orders.
    Tweak as needed for your locale.
    """
    out = {k: None for k in RETURN_FIELDS}

    if not formatted_address:
        return out

    out['formatted_address'] = formatted_address

    # Normalize separators and remove excessive whitespace
    addr = re.sub(r'\s+', ' ', formatted_address.replace('\n', ', ')).strip()
    # remove " — " style site extras
    addr = re.sub(r'\s*—\s*', ', ', addr)

    # remove any "Open: " or phone parts (simple heuristics)
    addr = re.sub(r'Phone[:\s]*\+?\d[\d\s\-().]+', '', addr)

    # Split by comma and strip
    parts = [p.strip() for p in re.split(r',|\n', addr) if p.strip()]
    # Remove trailing country names like India if present (common)
    if parts and parts[-1].lower() in ('india', 'united states', 'usa', 'u.s.a'):
        parts = parts[:-1]

    # try to find PIN (6-digit India) or other 4-6 digit tokens
    pin = None
    for i, part in enumerate(parts[::-1]):
        m = re.search(r'\b(\d{6})\b', part)
        if m:
            pin = m.group(1)
            # remove digits from that part so parsing is easier
            parts[len(parts)-1 - i] = re.sub(r'\b\d{6}\b', '', parts[len(parts)-1 - i]).strip()
            break
    if not pin:
        # try 5-digit or 4-digit
        for i, part in enumerate(parts[::-1]):
            m = re.search(r'\b(\d{4,6})\b', part)
            if m:
                pin = m.group(1)
                parts[len(parts)-1 - i] = re.sub(r'\b' + re.escape(pin) + r'\b', '', parts[len(parts)-1 - i]).strip()
                break
    out['pin'] = pin

    # heuristics for city/state:
    city = None
    state = None
    if parts:
        # if last part contains state-like keywords (two words, e.g., "Karnataka 560001" after removing pin)
        last = parts[-1]
        if last and len(parts) >= 2:
            # try split last by spaces: maybe "StateName PIN" or just "StateName"
            tokens = last.split()
            if len(tokens) <= 3 and not re.search(r'\d', last):
                # assume this is state or city if short
                # often last is "State" and previous is "City"
                state = last
                city = parts[-2] if len(parts) >= 2 else None
                # remove assigned parts
                parts = parts[:-2] if len(parts) >= 2 else []
            else:
                # else assume last is city/state combined; try previous as locality
                city = parts[-1]
                parts = parts[:-1]
        elif len(parts) == 1:
            city = parts[0]

    # If city remained None, try other heuristics
    if not city and len(parts) >= 2:
        city = parts[-1]
        parts = parts[:-1]

    out['city'] = city
    out['state'] = state

    # Street/area/locality heuristics:
    street = None
    area = None
    locality = None

    if parts:
        # If first part contains typical street words, treat as street
        street_candidates = []
        area_candidates = []
        for p in parts:
            if re.search(r'\b(Road|Rd|Street|St|Lane|Ln|Avenue|Ave|Block|Sector|Phase|Bazar|Bazaar|Market|Marg|Chowk|Nagar|Colony|Layout)\b', p, re.I):
                street_candidates.append(p)
            elif re.search(r'\b(Locality|Area|Ward|Zone)\b', p, re.I):
                area_candidates.append(p)
            else:
                area_candidates.append(p)

        if street_candidates:
            street = street_candidates[0]
            # assign first non-street as area/locality
            remainder = [p for p in parts if p != street]
            area = remainder[0] if remainder else None
            locality = remainder[1] if len(remainder) > 1 else None
        else:
            # fallback assignment
            street = parts[0]
            area = parts[1] if len(parts) > 1 else None
            locality = parts[2] if len(parts) > 2 else None

    out['street'] = street
    out['area'] = area
    out['locality'] = locality

    # Ensure all RETURN_FIELDS keys exist in output
    for k in RETURN_FIELDS:
        if k not in out:
            out[k] = None

    return out

# -----------------------------
# Main Selenium extractor
# -----------------------------
def Selenium_extractor(address, driver=None):
    """
    Given an address string, load Google Maps search and try to extract formatted address,
    lat/lng and components. Returns a dict with keys from RETURN_FIELDS.
    """
    own_driver = False
    if driver is None:
        driver = create_driver()
        own_driver = True

    result = {k: None for k in RETURN_FIELDS}
    try:
        search_url = "https://www.google.com/maps/search/" + quote_plus(address)
        driver.get(search_url)
        time.sleep(WAIT_AFTER_LOAD)

        # try to get lat/lng from the URL first
        cur_url = driver.current_url
        lat, lng = get_lat_lng_from_url(cur_url)
        # try clicking the first place result to get the formatted address in the details panel
        clicked_href = None
        try:
            # try to find place links (these are commonly like /place/...)
            a_candidates = driver.find_elements(By.XPATH, "//a[contains(@href, '/place/')]")
            if not a_candidates:
                # another common element: clickable divs with class 'hfpxzc'
                a_candidates = driver.find_elements(By.CLASS_NAME, "hfpxzc")
            if a_candidates:
                elem = a_candidates[0]
                # try to get href if present
                try:
                    clicked_href = elem.get_attribute("href")
                except Exception:
                    clicked_href = None
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", elem)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", elem)
                except Exception:
                    try:
                        elem.click()
                    except Exception:
                        pass
                time.sleep(CLICK_WAIT)
        except Exception:
            pass

        # after clicking, try to update lat/lng from URL/href
        if clicked_href and (not lat or not lng):
            lat, lng = get_lat_lng_from_url(clicked_href)
        if (not lat or not lng):
            lat2, lng2 = get_lat_lng_from_url(driver.current_url)
            if lat2 and lng2:
                lat, lng = lat2, lng2

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        formatted = find_formatted_address_from_soup(soup, page_source)

        parsed = parse_address_components(formatted)
        parsed['latitude'] = lat
        parsed['longitude'] = lng

        # ensure only the keys requested in RETURN_FIELDS are returned (plus formatted_address if requested)
        out = {k: parsed.get(k) for k in RETURN_FIELDS}
        return out

    except Exception as e:
        print(f"[ERROR] extracting '{address}': {e}")
        return {k: None for k in RETURN_FIELDS}
    finally:
        if own_driver:
            driver.quit()

# -----------------------------
# Batch runner
# -----------------------------
def process_csv(input_csv=INPUT_CSV, output_csv=OUTPUT_CSV, input_col=INPUT_COL, parallel=False, max_workers=3):
    df = pd.read_csv(input_csv, dtype=str, encoding="latin1")
    if input_col not in df.columns:
        raise ValueError(f"Input CSV column '{input_col}' not found. Columns: {df.columns.tolist()}")

    addresses = df[input_col].fillna('').astype(str).tolist()
    results = []

    if parallel:
        # run multiple drivers in parallel (be mindful of resource usage and Google limits)
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = []
            for addr in addresses:
                futures.append(ex.submit(Selenium_extractor, addr))
                time.sleep(QUERY_THROTTLE)
            for f in concurrent.futures.as_completed(futures):
                results.append(f.result())
    else:
        driver = create_driver()
        try:
            for addr in addresses:
                # try retries
                out = None
                for attempt in range(MAX_RETRIES):
                    out = Selenium_extractor(addr, driver=driver)
                    # consider success if at least formatted_address or lat found
                    if out and (out.get('formatted_address') or out.get('latitude') or out.get('pin')):
                        break
                    time.sleep(1 + attempt * 2)
                results.append(out or {k: None for k in RETURN_FIELDS})
                time.sleep(QUERY_THROTTLE)
        finally:
            driver.quit()

    # Merge with original dataframe
    results_df = pd.DataFrame(results)
    combined = pd.concat([df.reset_index(drop=True), results_df.reset_index(drop=True)], axis=1)
    combined.to_csv(output_csv, index=False, encoding='utf-8')
    print(f"Saved {len(combined)} rows to {output_csv}")

# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Google Maps address parser")
    parser.add_argument("--input", "-i", default=INPUT_CSV, help="Input CSV file path")
    parser.add_argument("--output", "-o", default=OUTPUT_CSV, help="Output CSV file path")
    parser.add_argument("--col", "-c", default=INPUT_COL, help="Column name that contains addresses")
    parser.add_argument("--parallel", "-p", action="store_true", help="Run addresses in parallel (multi-driver, resource heavy)")
    parser.add_argument("--workers", "-w", type=int, default=3, help="Number of parallel workers if using --parallel")
    args = parser.parse_args()

    process_csv(input_csv=args.input, output_csv=args.output, input_col=args.col, parallel=args.parallel, max_workers=args.workers)
