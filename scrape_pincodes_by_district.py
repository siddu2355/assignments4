#!/usr/bin/env python3
"""
Scrape all post-office names (and PINs if available) for a given state+district from pincode.net.in.

Example usage:
    python scrape_pincodes_by_district.py --state "Uttar Pradesh" --district "Ghaziabad" --output ghaziabad.csv
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import argparse, re, csv, time, random

USER_AGENT = "Mozilla/5.0 (compatible; PinScraper/1.0; +https://example.com/bot)"

LETTERS = [chr(c) for c in range(ord('A'), ord('Z') + 1)] + [str(i) for i in range(10)]

def to_component(name: str) -> str:
    """Convert human-readable name to site URL component: spaces/hyphens -> '_' and uppercase."""
    return re.sub(r'[\s\-]+', '_', name.strip()).upper()

def get_soup(session: requests.Session, url: str, timeout=20):
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser"), resp

def find_letter_links_from_district_page(soup: BeautifulSoup, state_comp: str, district_comp: str, base_url: str):
    """
    Find first link for each letter on the district page.
    Links typically look like: /STATE/DISTRICT/A/SomePostOffice
    Return dict: { 'A': 'https://pincode.net.in/.../A/SomePostOffice', ... }
    """
    pattern = re.compile(fr"/{re.escape(state_comp)}/{re.escape(district_comp)}/([A-Z0-9])/", re.I)
    links = {}
    for a in soup.find_all("a", href=True):
        m = pattern.search(a['href'])
        if m:
            letter = m.group(1).upper()
            if letter not in links:
                links[letter] = urljoin(base_url, a['href'])
    return links

def extract_post_offices_from_soup(soup: BeautifulSoup, state_comp: str = None, district_comp: str = None):
    """
    Attempt multiple strategies to collect post office names + pincodes:
    1) <select><option> dropdowns
    2) table rows
    3) anchor links that look like PO links
    Returns list of tuples: (post_office_name, pincode_or_None)
    """
    offices = []

    # Strategy 1: look for select dropdowns with post-office options
    for sel in soup.find_all("select"):
        opts = []
        for opt in sel.find_all("option"):
            txt = opt.text.strip()
            if not txt:
                continue
            if "select" in txt.lower():
                continue
            # Sometimes options include "OfficeName - 560xxx" so parse pin if present
            pin = re.search(r'\b\d{6}\b', txt)
            pin_val = pin.group() if pin else None
            # Clean name from trailing pin if present
            name = re.sub(r'\b\d{6}\b', '', txt).strip(" -\t\r\n")
            opts.append((name, pin_val))
        if opts:
            offices.extend(opts)
            # assume the dropdown contains all offices for the letter; keep adding other selects too
    if offices:
        return offices

    # Strategy 2: tables (many pages present PO name and pincode in a table)
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td", "th"])
            if not tds:
                continue
            # ignore header-like rows
            if any(th.name == 'th' for th in tds):
                # header row -> skip
                continue
            cols = [td.get_text(separator=" ", strip=True) for td in tds]
            if not cols:
                continue
            # heuristics:
            name = cols[0]
            pin = None
            # look for 6-digit in other columns
            for c in cols[1:3]:
                m = re.search(r'\b\d{6}\b', c)
                if m:
                    pin = m.group()
                    break
            # fallback search in whole row
            if not pin:
                m = re.search(r'\b\d{6}\b', " ".join(cols))
                if m:
                    pin = m.group()
            if name and len(name) > 1 and not name.lower().startswith("s.no"):
                offices.append((name, pin))

    if offices:
        return offices

    # Strategy 3: anchors that look like post-office links
    if state_comp and district_comp:
        for a in soup.find_all("a", href=True):
            href = a['href']
            text = a.get_text(strip=True)
            if not text or len(text) < 2:
                continue
            # prefer anchors whose href contains the state/district path
            if f"/{state_comp}/{district_comp}/" in href:
                pin_m = re.search(r'\b\d{6}\b', text)
                pin = pin_m.group() if pin_m else None
                name = re.sub(r'\b\d{6}\b', '', text).strip(" -")
                offices.append((name, pin))

    # final fallback: any distinct text tokens that look like PO names (very permissive)
    if not offices:
        for tag in soup.find_all(text=True):
            txt = tag.strip()
            if not txt:
                continue
            # skip nav/label single letters
            if re.fullmatch(r'[A-Z]', txt):
                continue
            # if text looks like "OfficeName - 560xxx" or just a normal name
            if len(txt) > 2 and not txt.lower().startswith(("select", "show")):
                pin = re.search(r'\b\d{6}\b', txt)
                offices.append((txt.strip(), pin.group() if pin else None))

    # deduplicate while preserving first pincode seen
    dedup = {}
    for name, pin in offices:
        n = re.sub(r'\s+', ' ', name).strip()
        if not n:
            continue
        if n not in dedup:
            dedup[n] = pin
    return list(dedup.items())

def scrape_district(state: str, district: str, output_file: str, delay_range=(0.4, 1.2), verbose=True):
    state_comp = to_component(state)
    district_comp = to_component(district)
    base = f"https://pincode.net.in/{state_comp}/{district_comp}/"

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    if verbose:
        print(f"[+] District base URL: {base}")

    try:
        soup, resp = get_soup(session, base)
    except Exception as e:
        print(f"[!] Failed to fetch district page: {e}")
        # as a fallback try without trailing slash
        try:
            soup, resp = get_soup(session, base.rstrip('/'))
        except Exception as e2:
            raise RuntimeError(f"Cannot reach district page: {e2}")

    # 1) find letter links from the district page
    letter_links = find_letter_links_from_district_page(soup, state_comp, district_comp, base)

    # If no letter links found, we'll attempt A-Z direct URLs
    if verbose:
        if letter_links:
            print(f"[+] Found letter entry points for letters: {', '.join(sorted(letter_links.keys()))}")
        else:
            print("[!] No explicit letter links discovered on district page; will try A-Z pages.")

    all_offices = {}
    # iterate known letters via links if found, else fall back to A-Z
    letters_to_try = sorted(letter_links.keys()) if letter_links else LETTERS

    for letter in letters_to_try:
        try:
            if letter in letter_links:
                url = letter_links[letter]
            else:
                # direct constructed URL. Some districts respond to /.../A/ while some need a trailing placeholder
                url = f"{base}{letter}/"
            if verbose:
                print(f"[>] Fetching letter {letter} -> {url}")

            soup_letter, _ = get_soup(session, url)
            offices = extract_post_offices_from_soup(soup_letter, state_comp, district_comp)

            # If the letter page returned nothing, try visiting the district base + letter without trailing slash
            if not offices and not letter in letter_links:
                try:
                    alt = f"{base}{letter}"
                    soup_letter, _ = get_soup(session, alt)
                    offices = extract_post_offices_from_soup(soup_letter, state_comp, district_comp)
                except Exception:
                    pass

            # store deduped offices
            for name, pin in offices:
                key = re.sub(r'\s+', ' ', name).strip()
                if not key:
                    continue
                if key not in all_offices:
                    all_offices[key] = pin

            # polite delay
            time.sleep(random.uniform(*delay_range))

        except requests.HTTPError as he:
            if verbose:
                print(f"[!] HTTP error for {letter}: {he}")
            continue
        except Exception as e:
            if verbose:
                print(f"[!] Error fetching/parsing {letter}: {e}")
            continue

    # Save to CSV
    if verbose:
        print(f"[+] Scraped {len(all_offices)} unique post offices. Writing to {output_file}")

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["post_office", "pincode"])
        for name, pin in sorted(all_offices.items(), key=lambda r: r[0].lower()):
            writer.writerow([name, pin if pin else ""])

    return all_offices

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Scrape pincode.net.in post offices for a district.")
    ap.add_argument("--state", required=True, help="State name (e.g. 'Uttar Pradesh' or 'UTTAR_PRADESH')")
    ap.add_argument("--district", required=True, help="District name (e.g. 'Ghaziabad' or 'GHAZIABAD')")
    ap.add_argument("--output", default="pincodes.csv", help="Output CSV file")
    ap.add_argument("--no-delay", action="store_true", help="Don't wait between requests (not recommended)")
    args = ap.parse_args()

    delay = (0.0, 0.0) if args.no_delay else (0.4, 1.0)
    results = scrape_district(args.state, args.district, args.output, delay_range=delay)
    print(f"[✓] Done. {len(results)} post offices saved to {args.output}")
