import concurrent.futures
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
import re

all_records = []

def scroll_left_panel(browser):
    panel_xpath = "//div[contains(@class, 'm6QErb') and contains(@class, 'DxyBCb') and contains(@class, 'XiKgde') and contains(@class, 'ecceSd')]"
    
    panels = browser.find_elements(By.XPATH, panel_xpath)
    panel = panels[1]
    last_height = browser.execute_script("return arguments[0].scrollHeight", panel)
    
    while True:
        browser.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", panel)
        time.sleep(3)

        new_height = browser.execute_script("return arguments[0].scrollHeight", panel)
        if new_height == last_height:
            break
        last_height = new_height

def extract_lat_lng(url):
    lat_lng_pattern = re.compile(r"!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)")
    match = lat_lng_pattern.search(url)
    
    if match:
        latitude = match.group(1)
        longitude = match.group(2)
        return latitude, longitude
    return None, None

def Selenium_extractor(place):
    link = "https://www.google.com/maps/search/pharmacy+stores+in+" + place
    browser = webdriver.Chrome()
    browser.get(str(link))
    time.sleep(10)
    scroll_left_panel(browser)
    
    action = ActionChains(browser)
    record_list = []
    a = browser.find_elements(By.CLASS_NAME, "hfpxzc")

    for i in range(len(a)):
        scroll_origin = ScrollOrigin.from_element(a[i])
        action.scroll_from_origin(scroll_origin, 0, 100).perform()
        action.move_to_element(a[i]).perform()
        browser.execute_script("arguments[0].click();", a[i])
        time.sleep(2)
        source = browser.page_source
        soup = BeautifulSoup(source, 'html.parser')
        try:
            Name_Html = soup.findAll('h1', {"class": "DUwDvf lfPIob"})
            if len(Name_Html) > 0:
                name = Name_Html[0].text
            else:
                name = "Name not found"

            phone = None
            address = "Address not found"
            website = "Not available"

            web_a = soup.find('a', {"class": "CsEnBe"})
            if web_a is not None and not web_a.get("href").startswith("https://business.google.com"):
                website = web_a.get("href")

            divs = soup.findAll('div', {"class": "Io6YTe fontBodyMedium kR99db fdkmkc"})
            if len(divs) > 0:
                address = divs[0].text
                for div in divs:
                    if div.text.startswith("+") or div.text.startswith("0"):
                        phone = div.text

            latitude, longitude = extract_lat_lng(a[i].get_attribute("href"))

            record_list.append({
                'Name': name,
                'Phone number': phone,
                'Address': address,
                'Website': website,
                'Latitude': latitude,
                'Longitude': longitude
            })

        except Exception as e:
            print(f"Error extracting data for {place}: {e}")

    browser.quit()
    return record_list

def run_parallel_scraping(place):
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        results = executor.map(Selenium_extractor, [place] * 3)
    
    global all_records
    for result in results:
        all_records.extend(result)


places = ["201012"]

for place in places:
    run_parallel_scraping(place)

    df = pd.DataFrame(all_records)
    df = df.drop_duplicates(subset=['Name', 'Address'])
    df.to_csv(f'{place}_pharmacies_results.csv', index=False, encoding='utf-8')