import threading
import time
import json
import csv
import random
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium.webdriver.remote.webdriver import WebDriver
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

from exceptions import AccessDeniedException, MaxRetryAttemptsReachedException
from utilities import get_UTC_timestamp




class Parser:
    """Class for parsing data from Avito."""

    def __init__(self, browser, max_workers=3, delay_range=(3, 5)):
        """
        Initialize the parser.
        :param browser: Callable to produce browser instances.
        :param max_workers: Number of threads to use in multithreading.
        :param delay_range: Range of delays between requests.
        """
        self.browser = browser
        self.max_workers = max_workers
        self.delay_range = delay_range
        self.lock = threading.Lock()

    def _get_json(self, driver: WebDriver, url: str, delay: int, max_attempts: int = 3) -> dict:
        """Fetch JSON data from a URL using Selenium."""
        attempts = 0
        while attempts < max_attempts:
            try:
                driver.get(url)
                print(f"Navigated to {driver.current_url}")
                time.sleep(delay)

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                pre_tag = soup.find('pre')
                if not pre_tag:
                    raise ValueError("No <pre> tag found.")
                data = json.loads(pre_tag.text)

                if data.get('status') == "too-many-requests":
                    raise AccessDeniedException("Too many requests. Access denied.")
                return data
            
            except (json.JSONDecodeError, TimeoutException, ValueError) as e:
                attempts += 1
                print(f"Attempt {attempts}/{max_attempts} failed: {e}")
        raise MaxRetryAttemptsReachedException("Max retry attempts reached.")


    def _parse_data(self, data: dict) -> list:
        """Parse JSON data into structured property objects."""
        properties = []
        for item in data.get('items', []):
            try:
                properties.append({
                    'id': item.get('id'),
                    'category': item.get('category', {}).get('slug'),
                    'title': item.get('title', 'N/A'),
                    'price': item.get('priceDetailed', {}).get('string', 'N/A'),
                    'price_for': item.get('priceDetailed', {}).get('postfix', 'на продажу'),
                    'location': item.get('location', {}).get('name', 'N/A'),
                    'photos': [img.get('864x864', '') for img in item.get('images', [])],
                    'URL': item.get('urlPath', ''),
                })
            except Exception as e:
                print(f"Error parsing item: {e}")
        return properties
    
    def get_urls(self,
                category_id: int,
                total_goal: int,
                limit: int,
                offset: int,
                base_url: str = 'https://www.avito.ru/web/1/main/items?',
                location: bool = False
                
    ) -> list:
        """
        Generate URLs for scraping.

        :param category_id: Category ID for the items.
        :param total_goal: Total number of items to fetch.
        :param limit: Number of items per request.
        :param offset: Starting offset for the requests.
        :param base_url: Base URL for the API.
        :param location: Location filter for the request.
        :return: List of URLs to scrape.
        """
            
        last_stamp = get_UTC_timestamp()
        urls = []
        while offset < total_goal:
            full_url = f'{base_url}forceLocation={location}&lastStamp={last_stamp}&limit={limit}&offset={offset}&categoryId={category_id}'
            urls.append(full_url)
            print(f"{full_url} added to the list.")
            offset += limit
        
        return urls
        

    def _worker(self, driver: WebDriver, url: str, output: list, delay: int):
        """Worker function for fetching and parsing data."""
        try:
            json_data = self._get_json(driver, url, delay)
            parsed_data = self._parse_data(json_data)
            with self.lock:
                output.extend(parsed_data)
                print(f"Added {len(parsed_data)} objects.")
        except Exception as e:
            print(f"Error in worker for {url}: {e}")
            driver.quit()

    def _run_thread_pool(self, urls: list, output: list):
        """Run the thread pool to process URLs."""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for url in urls:
                delay = random.randint(*self.delay_range)
                driver = self.browser()
                futures.append(executor.submit(self._worker, driver, url, output, delay))
            for future in as_completed(futures):
                future.result()
                driver.quit()
            


    def scrape(self, urls: list, multithreaded: bool = False) -> list:
        """Main scraping function."""
        output = []
        if multithreaded:
            self._run_thread_pool(urls, output)
        else:
            driver = self.browser()
            for url in urls:
                self._worker(driver, url, output, random.randint(*self.delay_range))
        return output

    def save_to_csv(self, data: list, path: str, filename: str):
        """Save data to a CSV file."""
        if not data:
            print("No data to save.")
            return
        filepath = os.path.join(path,filename)
        with open(filepath,'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print(f"Data saved to {filepath}.")


