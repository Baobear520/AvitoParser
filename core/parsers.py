import time
import json
import random

from selenium.webdriver.remote.webdriver import WebDriver
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

from core.enums import CategoryType
from core.exceptions import AccessDeniedException, MaxRetryAttemptsReachedException
from core.utilities import get_utc_timestamp, return_unique_records




class BaseParser:
    """Base class for parsing initial data from Avito."""

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
        :param category_name: The category being scraped (e.g., 'real_estate', 'vehicles', 'electronics','household_equipment').
        :return: Parsed object with the 'object_category' field included.
        """
        obj = {
            'id': item.get('id'),
            'category': category_name,
            'type': item.get('category', {}).get('slug'),
            'title': item.get('title', 'N/A'),
            'price': item.get('priceDetailed', {}).get('string', 'N/A'),
            'price_for': item.get('priceDetailed', {}).get('postfix', ''),
            'location': item.get('location', {}).get('name', 'N/A'),
            'photo_URLs': [
                img.get('864x864', img.get('640x640', None)) for img in item.get('images', [])
            ],
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
        :param category_name: The category being scraped (e.g., 'real_estate', 'vehicles', 'electronics','household_equipment').
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
            return parsed_data

        except MaxRetryAttemptsReachedException:
            print(f"Max retries reached for {url}. Moving to the next URL.")
            return []
        except AccessDeniedException as e:
            print(f"Access denied for {url}. Stopping the script.")
            raise e  # Re-raise the exception to stop the scraping process
        except Exception as e:
            print(f"Unexpected error in worker for {url}: {e}")
            return []


    def fetch_category_objects(self, driver, category, limit, offset, last_stamp, location):
        """Fetch objects for a specific category."""

        url = self.url_generator(category.category_id, limit, offset, last_stamp, location)
        print(f"Fetching data from: {url} for category: {category.verbose_name}")
        return self._worker(driver, url, category.verbose_name, random.randint(*self.delay_range))


    def run(self, driver, total_goal, limit, location=False, max_scraping_failures=3):
        """
        Fetch objects dynamically until total_goal is met.
        :parameters:
            - driver: WebDriver instance
            - total_goal: Total number of objects to fetch
            - limit: Number of objects per API call
            - delay: Delay between API requests
            - location: Location filter for the request
            - max_scraping_failures: Maximum number of consecutive zero-fetch attempts
        """

        fetched_objects = []
        offset = 0
        last_stamp = get_utc_timestamp()

        # Initialize the zero-fetch counter to prevent infinite loops
        scraping_failures_count = 0

        while len(fetched_objects) < total_goal:
            try:
                for category in CategoryType:
                    new_objects = self.fetch_category_objects(driver, category, limit, offset, last_stamp, location)
                    if new_objects:
                        scraping_failures_count = 0
                        fetched_objects.extend(new_objects)
                        print(f"Added {len(new_objects)} objects. Total: {len(fetched_objects)}/{total_goal}.")
                    else:
                        scraping_failures_count += 1
                        print(
                            f"No objects fetched for category: {category.verbose_name}."
                            f"\nZero-fetch count: {scraping_failures_count}/{max_scraping_failures}.")

                    if len(fetched_objects) >= total_goal:
                        print(f"Goal reached: {len(fetched_objects)} objects fetched.")
                        return return_unique_records(fetched_objects)

                    if scraping_failures_count >= max_scraping_failures:
                        print(
                            f"Too many consecutive zero-fetch attempts ({scraping_failures_count}). Stopping script.")
                        return return_unique_records(fetched_objects)

                offset += limit * 2

            except AccessDeniedException:
                print("Access Denied Exception raised. Stopping script.")
                break

        return return_unique_records(fetched_objects)










