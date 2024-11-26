import os
import csv
import json
import logging
from urllib.parse import urlencode
from selenium import webdriver
from selenium.webdriver.common.by import By
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]

options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--disable-javascript")


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def crawl_profiles(name, location, retries=3):
    first_name = name.split()[0]
    last_name = name.split()[1]
    url = f"https://www.linkedin.com/pub/dir?firstName={first_name}&lastName={last_name}&trk=people-guest_people-search-bar_search-submit"
    tries = 0
    success = False
    
    while tries <= retries and not success:

        driver = webdriver.Chrome(options=options)

        try:
            driver.get(url)
                
            profile_cards = driver.find_elements(By.CSS_SELECTOR, "div[class='base-search-card__info']")
            for card in profile_cards:
                parent = card.find_element(By.XPATH, "..")
                href = parent.get_attribute("href").split("?")[0]
                name = href.split("/")[-1].split("?")[0]
                display_name = card.find_element(By.CSS_SELECTOR,"h3[class='base-search-card__title']").text
                location = card.find_element(By.CSS_SELECTOR, "p[class='people-search-card__location']").text
                companies = "n/a"
                has_companies = card.find_elements(By.CSS_SELECTOR, "span[class='entity-list-meta__entities-list']")
                if has_companies:
                    companies = has_companies[0].text

                search_data = {
                    "name": name,
                    "display_name": display_name,
                    "url": href,
                    "location": location,
                    "companies": companies
                }
            
                print(search_data)
            logger.info(f"Successfully parsed data from: {url}")
            success = True        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
            tries+=1
        
        finally:
            driver.quit()

    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


def start_crawl(profile_list, location, retries=3):
    for name in profile_list:
        crawl_profiles(name, location, retries=retries)



if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["bill gates", "elon musk"]

    ## Job Processes
    filename = "profile-crawl.csv"
    start_crawl(keyword_list, LOCATION, retries=MAX_RETRIES)
    logger.info(f"Crawl complete.")