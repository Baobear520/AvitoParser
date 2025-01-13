import threading
import time
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium.webdriver.remote.webdriver import WebDriver
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

from core.enums import CategoryType
from core.exceptions import AccessDeniedException, MaxRetryAttemptsReachedException
from core.utilities import get_utc_timestamp, return_unique_records




class Parser:
    """Class for parsing data from Avito."""

    def __init__(self, browser, base_url, max_workers=3, delay_range=(5, 15)):
        """
        Initialize the parser.
        :param browser: Callable to produce browser instances.
        :param max_workers: Number of threads to use in multithreading.
        :param delay_range: Range of delays between requests.
        """
        self.browser = browser
        self.max_workers = max_workers
        self.base_url = base_url
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


    def _parse_item(self, item, category_name):
        """
        Parse an individual item into a structured object.

        :param item: Raw item data from the API response.
        :param category_name: The category being scraped (e.g., 'real_estate', 'vehicles', 'electronics').
        :return: Parsed object with the 'object_category' field included.
        """
        obj = {
            'id': item.get('id'),
            'object_category': category_name,  # Add the category being scraped
            'subcategory': item.get('category', {}).get('slug'),
            'title': item.get('title', 'N/A'),
            'price': item.get('priceDetailed', {}).get('string', 'N/A'),
            'price_for': item.get('priceDetailed', {}).get('postfix', ''),
            'location': item.get('location', {}).get('name', 'N/A'),
            'photo_URLs': [img.get('864x864', '640x640') for img in item.get('images', [])],
            'source_URL': item.get('urlPath', '')
        }

        # Default value for price_for if it's empty
        if obj['price_for'] == "":
            obj['price_for'] = "на продажу"

        return obj

    def _parse_data(self, data, category_name):
        """
        Parse a list of raw data items into structured objects with 'object_category'.

        :param data: JSON data containing raw items from the API.
        :param category: The category being scraped (e.g., 'real_estate', 'vehicles', 'electronics').
        :return: List of parsed objects.
        """
        objects = []
        for item in data.get('items', []):
            try:
                obj = self._parse_item(item, category_name)
                objects.append(obj)
            except Exception as e:
                print(f"Error parsing item: {e}")
        return objects
    
    def get_urls(self,
                category: CategoryType,
                total_goal: int,
                limit: int,
                offset: int,
                base_url: str,
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

    def url_generator(self, category_id, limit, offset, last_stamp, location):
        """
        Dynamically generate the next URL based on the current offset.

        :param category_id: The category being scraped (e.g., 'real_estate').
        :param limit: Number of objects to fetch per request.
        :param offset: Current offset for pagination.
        :param last_stamp: Timestamp for the last request.
        :param location: Location filter for the request.
        :return: Generated URL.
        """
        return f"{self.base_url}?forceLocation={location}&lastStamp={last_stamp}&limit={limit}&offset={offset}&categoryId={category_id}"

    def _worker(self, driver: WebDriver, url: str, category_name: CategoryType, delay: int):
        """Worker function for fetching and parsing data."""
        try:
            json_data = self._get_json(driver, url, delay)
            parsed_data = self._parse_data(json_data, category_name)
            # with self.lock:
            #     output.extend(parsed_data)
            #     print(f"Added {len(parsed_data)} objects.")
            return parsed_data

        except MaxRetryAttemptsReachedException as e:
            print(f"Max retries reached for {url}. Moving to the next URL.")
        except AccessDeniedException as e:
            print(f"Access denied for {url}. Stopping the script.")
            raise e  # Re-raise the exception to stop the scraping process
        except Exception as e:
            print(f"Unexpected error in worker for {url}: {e}")

    def fetch_objects(self, driver, total_goal, limit, location=False):
        """
        Fetch objects dynamically until total_goal is met.
        :parameters:
            - driver: WebDriver instance
            - total_goal: Total number of objects to fetch
            - limit: Number of objects per API call
            - delay: Delay between API requests
            - location: Location filter for the request
        """

        fetched_objects = []
        offset = 0
        last_stamp = get_utc_timestamp()

        while len(fetched_objects) < total_goal:
            try:
                for category in CategoryType:
                    # Generate the next URL dynamically
                    url = self.url_generator(category.category_id, limit, offset, last_stamp, location)
                    print(f"Fetching data from: {url}")

                    # Fetch and parse data from the current URL
                    new_objects = self._worker(driver,url,category.verbose_name,random.randint(*self.delay_range))
                    fetched_objects.extend(new_objects)
                    print(f"Added {len(new_objects)} objects.")
                offset += limit * 2
            except AccessDeniedException:
                print("Access Denied Exception raised. Stopping script.")
                break  # Stop the loop when access is denied

        return return_unique_records(fetched_objects)

    def _run_thread_pool(self, category: int, urls: list, output: list):
        """Helper function to run the workers in a thread pool."""
        with ThreadPoolExecutor(max_workers=5) as executor:
            driver = self.browser()
            delay = random.randint(*self.delay_range)
            futures = [executor.submit(
                self._worker, driver, url, output, category,delay) for url in urls]
            
            for future in as_completed(futures):
                try:
                    future.result()  # Wait for the result to be available
                except AccessDeniedException:
                    print("Access Denied Exception raised in thread. Stopping script.")
                    break  # Stop further execution in the thread pool if access is denied


    def scrape(self, category: int, urls: list, multithreaded: bool = False) -> list:
        """Main scraping function."""
        
        output = []
        if multithreaded:
            self._run_thread_pool(category, urls, output)
        else:
            driver = self.browser
            for url in urls:
                try:
                    self._worker(driver, url, output, category, random.randint(*self.delay_range))
                except AccessDeniedException:
                    print("Access Denied Exception raised. Stopping script.")
                    break  # Stop the loop when access is denied
        print(f"Total: {len(output)} objects.")
        return return_unique_records(output)






