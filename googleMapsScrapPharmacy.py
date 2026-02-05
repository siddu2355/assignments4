import concurrent.futures
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import logging
from datetime import datetime

all_records = []
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_browser(use_profile=False):
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-images')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.page_load_strategy = 'eager'
    
    if use_profile:
        chrome_options.add_argument('--user-data-dir=C:/Users/sathw/AppData/Local/Google/Chrome/User Data')
        chrome_options.add_argument('--profile-directory=Default')
        chrome_options.add_argument('--disable-features=VizDisplayCompositor')
    
    try:
        browser = webdriver.Chrome(options=chrome_options)
        browser.set_page_load_timeout(30)
        return browser
    except Exception as e:
        logger.error(f"Failed to setup browser: {e}")
        raise

def scroll_left_panel_optimized(browser):
    try:
        wait = WebDriverWait(browser, 15)
        
        panel_xpath = "//div[contains(@class, 'm6QErb') and contains(@class, 'DxyBCb') and contains(@class, 'XiKgde') and contains(@class, 'ecceSd')]"
        
        panels = browser.find_elements(By.XPATH, panel_xpath)
        if len(panels) < 2:
            logger.warning("Panel not found")
            return
            
        panel = panels[1]
        
        no_more_results = False
        scroll_attempts = 0
        max_attempts = 100
        last_count = 0
        stable_count = 0
        
        while not no_more_results and scroll_attempts < max_attempts:
            last_height = browser.execute_script("return arguments[0].scrollHeight", panel)
            browser.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", panel)
            
            time.sleep(2)
            
            new_height = browser.execute_script("return arguments[0].scrollHeight", panel)
            current_elements = browser.find_elements(By.CLASS_NAME, "hfpxzc")
            current_count = len(current_elements)
            
            logger.info(f"Scroll attempt {scroll_attempts + 1}: Found {current_count} elements")
            
            if current_count == last_count:
                stable_count += 1
                if stable_count >= 3:
                    time.sleep(3)
                    new_height = browser.execute_script("return arguments[0].scrollHeight", panel)
                    if new_height == last_height:
                        no_more_results = True
                        logger.info(f"No more results after {scroll_attempts + 1} attempts. Total elements: {current_count}")
            else:
                stable_count = 0
                last_count = current_count
            
            scroll_attempts += 1
            
    except Exception as e:
        logger.error(f"Error scrolling panel: {e}")

def extract_lat_lng(url):
    lat_lng_pattern = re.compile(r"!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)")
    match = lat_lng_pattern.search(url)
    
    if match:
        latitude = match.group(1)
        longitude = match.group(2)
        return latitude, longitude
    return None, None

def Selenium_extractor_optimized(place, search_term="pharmacy", use_profile=False):
    browser = None
    try:
        browser = setup_browser(use_profile)
        
        if use_profile:
            browser.get("https://accounts.google.com")
            time.sleep(3)
            logger.info("Browser opened with existing profile - check if logged in")
        
        link = f"https://www.google.com/maps/search/{search_term}+stores+in+" + place
        browser.get(link)
        
        time.sleep(5)
        scroll_left_panel_optimized(browser)
        
        record_list = []
        a = browser.find_elements(By.CLASS_NAME, "hfpxzc")
        total_elements = len(a)
        logger.info(f"Total elements found for '{search_term}': {total_elements}")
        
        for i in range(total_elements):
            try:
                elements = browser.find_elements(By.CLASS_NAME, "hfpxzc")
                if i >= len(elements):
                    logger.warning(f"Element {i} no longer available, skipping")
                    continue
                    
                element = elements[i]
                browser.execute_script("arguments[0].scrollIntoView(true);", element)
                time.sleep(0.5)
                
                wait = WebDriverWait(browser, 15)
                
                max_retries = 3
                for retry in range(max_retries):
                    try:
                        element.click()
                        break
                    except Exception as click_error:
                        if retry == max_retries - 1:
                            browser.execute_script("arguments[0].click();", element)
                        else:
                            time.sleep(1)
                
                time.sleep(1.5)
                source = browser.page_source
                soup = BeautifulSoup(source, 'html.parser')
                
                Name_Html = soup.find_all('h1', {"class": "DUwDvf lfPIob"})
                name = Name_Html[0].text if Name_Html else "Name not found"
                
                phone = None
                address = "Address not found"
                website = "Not available"
                
                web_a = soup.find('a', {"class": "CsEnBe"})
                if web_a is not None and not web_a.get("href", "").startswith("https://business.google.com"):
                    website = web_a.get("href")
                
                divs = soup.find_all('div', {"class": "Io6YTe fontBodyMedium kR99db fdkmkc"})
                if divs:
                    address = divs[0].text
                    for div in divs:
                        if div.text.startswith("+") or div.text.startswith("0"):
                            phone = div.text
                
                latitude, longitude = extract_lat_lng(element.get_attribute("href"))
                
                record_list.append({
                    'Name': name,
                    'Phone number': phone,
                    'Address': address,
                    'Website': website,
                    'Latitude': latitude,
                    'Longitude': longitude,
                    'Search Term': search_term
                })
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{total_elements} records for '{search_term}'")
                
            except Exception as e:
                logger.warning(f"Error extracting item {i} for '{search_term}': {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error in extractor for {place}: {e}")
    finally:
        if browser:
            try:
                browser.quit()
            except:
                pass
    
    return record_list

def run_optimized_scraping(place, use_profile=False):
    logger.info(f"Starting comprehensive scraping for {place}")
    
    search_terms = ["pharmacy", "medical store", "chemist", "medicine shop", "drug store"]
    all_search_results = []
    
    try:
        for search_term in search_terms:
            logger.info(f"Searching for: {search_term}")
            record_list = Selenium_extractor_optimized(place, search_term, use_profile)
            if record_list:
                all_search_results.extend(record_list)
            
            time.sleep(2)
        
        global all_records
        all_records.extend(all_search_results)
        
        logger.info(f"Total extracted records from all searches: {len(all_search_results)}")
        
    except Exception as e:
        logger.error(f"Failed to scrape {place}: {e}")

places = ["201014"]

start_time = time.time()

for place in places:
    all_records = []
    run_optimized_scraping(place, use_profile=False)
    
    if all_records:
        df = pd.DataFrame(all_records)
        df = df.drop_duplicates(subset=['Name', 'Address'])
        
        valid_records = df[df['Name'] != 'Name not found']
        valid_records = valid_records[valid_records['Address'] != 'Address not found']
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        df.to_csv(f'{place}_pharmacy_results_{timestamp}.csv', index=False, encoding='utf-8')
        valid_records.to_csv(f'{place}_pharmacy_results_valid_{timestamp}.csv', index=False, encoding='utf-8')
        
        logger.info(f"Saved {len(df)} total records to {place}_pharmacy_results_{timestamp}.csv")
        logger.info(f"Saved {len(valid_records)} valid records to {place}_pharmacy_results_valid_{timestamp}.csv")
        
        logger.info(f"Consistency check - Total unique names: {len(df['Name'].unique())}")
        logger.info(f"Consistency check - Total unique addresses: {len(df['Address'].unique())}")
    else:
        logger.warning(f"No records found for {place}")

end_time = time.time()
logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")