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

from core.enums import CategoryType
from core.exceptions import AccessDeniedException, MaxRetryAttemptsReachedException
from core.utilities import get_utc_timestamp, return_unique_records




class Parser:
    """Class for parsing data from Avito."""

    def __init__(self, browser, max_workers=3, delay_range=(5, 15)):
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
                print(f"Attempt {attempts}/{max_attempts} failed for {url}")
        raise MaxRetryAttemptsReachedException("Max retry attempts reached.")


    def _parse_data(self, data: dict) -> list:
        """Parse JSON data into structured property objects."""
        properties = []
        for item in data.get('items', []):
            try:
                obj = {
                    'id': item.get('id'),
                    'category': item.get('category', {}).get('slug'),
                    'title': item.get('title', 'N/A'),
                    'price': item.get('priceDetailed', {}).get('string', 'N/A'),
                    'price_for': item.get('priceDetailed', {}).get('postfix', ''),
                    'location': item.get('location', {}).get('name', 'N/A'),
                    'photo_URLs': [img.get('864x864', '') for img in item.get('images', [])],
                    'source_URL': item.get('urlPath', '')
                }
                if obj['price_for'] == "":
                    obj['price_for'] = "на продажу"
                properties.append(obj)
            except Exception as e:
                print(f"Error parsing item: {e}")
        return properties
    
    def get_urls(self,
                category: CategoryType,
                total_goal: int,
                limit: int,
                offset: int,
                base_url: str = 'https://www.avito.ru/web/1/main/items?',
                location: bool = False
                
    ) -> list:
        """
        Generate URLs for scraping.

        :param category: Category enum instance.
        :param total_goal: Total number of items to fetch.
        :param limit: Number of items per request.
        :param offset: Starting offset for the requests.
        :param base_url: Base URL for the API.
        :param location: Location filter for the request.
        :return: List of URLs to scrape.
        """
            
        last_stamp = get_utc_timestamp()
        category_id = category.category_id  # Get numeric ID from the enum

        urls = []
        while offset < total_goal * 2:
            full_url = f'{base_url}forceLocation={location}&lastStamp={last_stamp}&limit={limit}&offset={offset}&categoryId={category_id}'
            urls.append(full_url)
           
            offset += limit * 2
        print(f"{len(urls)} URLs have been added to the list.")
        return urls
        

    def _worker(self, driver: WebDriver, url: str, output: list, delay: int):
        """Worker function for fetching and parsing data."""
        try:
            json_data = self._get_json(driver, url, delay)
            parsed_data = self._parse_data(json_data)
            with self.lock:
                output.extend(parsed_data)
                print(f"Added {len(parsed_data)} objects.")

        except MaxRetryAttemptsReachedException as e:
            print(f"Max retries reached for {url}. Moving to the next URL.")
        except AccessDeniedException as e:
            print(f"Access denied for {url}. Stopping the script.")
            raise e  # Re-raise the exception to stop the scraping process
        except Exception as e:
            print(f"Unexpected error in worker for {url}: {e}")


    def _run_thread_pool(self, urls: list, output: list):
        """Helper function to run the workers in a thread pool."""
        with ThreadPoolExecutor(max_workers=5) as executor:
            driver = self.browser()
            delay = random.randint(*self.delay_range)
            futures = [executor.submit(
                self._worker, driver, url, output, delay) for url in urls]
            
            for future in as_completed(futures):
                try:
                    future.result()  # Wait for the result to be available
                except AccessDeniedException:
                    print("Access Denied Exception raised in thread. Stopping script.")
                    break  # Stop further execution in the thread pool if access is denied


    def scrape(self, urls: list, multithreaded: bool = False) -> list:
        """Main scraping function."""
        
        output = []
        if multithreaded:
            self._run_thread_pool(urls, output)
        else:
            driver = self.browser()  # This is where you initialize your driver
            for url in urls:
                try:
                    self._worker(driver, url, output, random.randint(*self.delay_range))
                except AccessDeniedException:
                    print("Access Denied Exception raised. Stopping script.")
                    break  # Stop the loop when access is denied
        print(f"Total: {len(output)} objects.")
        return return_unique_records(output)


    def save_to_csv(self, data: list[dict], path: str, filename: str):
        """Save data to a CSV file."""
        if not data:
            print("No data to save.")
            return
        filepath = os.path.join(path,filename)
        # Check if the file already exists
        if os.path.exists(filepath):
            new_data = self._read_unique_data_from_csv(filepath, data)
            if new_data:
                with open(filepath,'a', newline='', encoding='utf-8') as file:
                    writer = csv.DictWriter(file, fieldnames=data[0].keys())
                    writer.writerows(new_data)
                    print(f"Data appended to {filepath}.")
        else:
            with open(filepath,'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            print(f"Created file {filepath} and saved data.")


    def _read_unique_data_from_csv(self, filepath, data):
        existing_data = set()
        with open(filepath, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            # Add existing rows to the set based on the unique field
            for row in reader:
                existing_data.add(row['id'])
        # Filter out the rows that already exist in the CSV
        new_data = [row for row in data if row['id'] not in existing_data]
        if not new_data:
            print("No new unique data to save.")
            return
        print(f"New data: {len(new_data)} objects")
        return new_data



