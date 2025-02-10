import time
import json
import random

from selenium.webdriver.remote.webdriver import WebDriver
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from faker import Faker

from core.enums import CategoryType
from core.exceptions import AccessDeniedException, MaxRetryAttemptsReachedException
from core.utilities import get_utc_timestamp, return_unique_records

from main_scripts.download_photos import download_and_save_photos

class BaseParser:
    """Base class for parsing initial data from Avito."""

    def __init__(self, browser, base_url, delay_range=(5, 15)):
        """
        Initialize the parser.
        :param base_url: Base URL of the site.
        :param delay_range: Range of delays between requests.
        """
        self.browser = browser
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
                print(f"Attempt {attempts}/{max_attempts} failed for {url}. Retrying...")
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

    def fetch_objects_for_category(self, driver, category, limit, offset, last_stamp, location):
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
                    new_objects = self.fetch_objects_for_category(driver, category, limit, offset, last_stamp, location)
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


class DailyParser(BaseParser):
    def __init__(self, db, browser, base_url, user_count, delay_range=(5, 10)):
        super().__init__(browser, base_url, delay_range)
        self.db = db
        self.user_count = user_count
        self.faker = Faker()

    def get_existing_object_ids(self, table_name, category_name):
        """Fetch all existing object IDs for a given category."""
        with self.db.conn as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT id FROM {table_name} WHERE category = %s;", (category_name,))
                return {row[0] for row in cursor.fetchall()}

    def check_for_unique_in_db(self, table_name, category_name):
        """
        Fetch unique objects from the database table for a specific category.

        :param table_name: Name of the table to query (e.g., 'unique_records' or 'objects').
        :param category_name: The category to filter objects by.
        :return: List of objects from the specified table matching the category.
        """
        try:
            with self.db.conn as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT id, category, type, title, price, price_for, location, photo_URLs, source_URL, last_updated
                        FROM {table_name}
                        WHERE category = %s;
                    """, (category_name,))
                    rows = cursor.fetchall()

                    # Convert rows into dictionaries
                    return [
                        {
                            'id': row[0],
                            'category': row[1],
                            'type': row[2],
                            'title': row[3],
                            'price': row[4],
                            'price_for': row[5],
                            'location': row[6],
                            'photo_URLs': row[7],
                            'source_URL': row[8],
                            'last_updated': row[9]
                        }
                        for row in rows
                    ]
        except Exception as e:
            print(f"Error fetching unique objects from {table_name}: {e}")
            return []


    def remove_assigned_objects_from_unique_records(self, table_name, assigned_object_ids):
        """Batch removal of assigned objects from unique_records."""
        if not assigned_object_ids:
            return
        with self.db.conn as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    DELETE FROM {table_name}
                    WHERE id = ANY(%s);
                """, (list(assigned_object_ids),))
            print(f"Removed {len(assigned_object_ids)} objects from '{table_name}'.")

    def generate_user_data(self):
        """Generate random user data."""
        gender = self.faker.random_element(["M", "F"])
        return {
            "username": self.faker.user_name(),
            "phone_number": self.faker.phone_number()[:32],  # Truncate to avoid VARCHAR(32) overflow
            "email": self.faker.email(),
            "first_name": self.faker.first_name_male() if gender == "M" else self.faker.first_name_female(),
            "last_name": self.faker.last_name(),
            "address": self.faker.address(),
            "gender": gender,
        }

    def save_user_and_objects(self, user_data, assigned_objects):
        """Save user and assigned objects in a single transaction."""
        try:
            with self.db.conn as conn:
                with conn.cursor() as cursor:
                    conn.autocommit = False

                    # Save user
                    cursor.execute("""
                        INSERT INTO users (username, phone_number, email, first_name, last_name, address, gender)
                        VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
                    """, (
                        user_data['username'],
                        user_data['phone_number'],
                        user_data['email'],
                        user_data['first_name'],
                        user_data['last_name'],
                        user_data['address'],
                        user_data['gender']
                    ))

                    user_id = cursor.fetchone()[0]

                    # Update assigned objects with user_id
                    object_ids = [obj['id'] for obj in assigned_objects]
                    cursor.execute("""
                        UPDATE objects
                        SET user_id = %s
                        WHERE id = ANY(%s);
                    """, (user_id, object_ids))

                    conn.commit()
                    print(f"Saved user {user_data['username']} and assigned {len(object_ids)} objects.")
        except Exception as e:
            print(f"Error saving user and objects: {e}")
            if conn:
                conn.rollback()

    def run(self, driver, total_goal, limit, location=False,max_scraping_failures=3):
        """Main logic to generate users, assign objects, and manage unique_records.

        - Generate users
        - Assign objects to users
        - Remove assigned objects from unique_records
        - Save user and assigned objects

        :param driver: Selenium WebDriver
        :param total_goal: Total number of objects for a user per category
        :param limit: Number of objects per API call
        :param location: Location filter for the request
        :param max_scraping_failures: Maximum number of consecutive zero-fetch attempts

        """
        for user in range(self.user_count):
            user_data = self.generate_user_data()
            print(f"Generating user {user_data['username']}...")
            assigned_objects_per_user = []

            for category in CategoryType:
                print(f"Current category: {category.verbose_name}")
                assigned_objects_per_category = []
                unique_objects = self.check_for_unique_in_db('unique_records', category.verbose_name)

                if unique_objects:
                    print(f"Found {len(unique_objects)} unique objects for {category.verbose_name}.")
                    objects_to_assign = unique_objects[:(min(total_goal, len(unique_objects)))]
                    assigned_objects_per_category.extend(objects_to_assign)
                    assigned_objects_per_user.extend(objects_to_assign)
                    # Remove assigned objects from unique_records
                    print(f"Assigned {len(objects_to_assign)} objects for {category.verbose_name}.")
                    self.remove_assigned_objects_from_unique_records('unique_records', [obj['id'] for obj in objects_to_assign])

                    print(f"Done for {category.verbose_name}.")
                    print("-" * 50)
                else:
                    print(f"No unique objects for {category.verbose_name}. Fetching new objects...")
                    offset = 0
                    last_stamp = get_utc_timestamp()
                    while len(assigned_objects_per_category) < total_goal:
                        new_objects = self.fetch_objects_for_category(driver, category, limit, offset, last_stamp, location)
                        print(f"Fetched {len(new_objects)} new objects for {category.verbose_name}.")
                        existing_ids = self.get_existing_object_ids('objects', category.verbose_name)
                        # Filter out existing objects
                        unique_objects = [obj for obj in new_objects if obj['id'] not in existing_ids]
                        print(f"Filtered {len(unique_objects)} unique objects for {category.verbose_name}.")

                        if unique_objects:
                            objects_to_assign = unique_objects[:min(total_goal, len(unique_objects))]
                            print(f"Assigning {len(objects_to_assign)} objects for {category.verbose_name}.")
                            assigned_objects_per_category.extend(objects_to_assign)
                            assigned_objects_per_user.extend(objects_to_assign)

                            remaining_objects = unique_objects[total_goal:]
                            print(f"Remaining {len(remaining_objects)} unique objects for {category.verbose_name} after assignment.")

                            print(f"Saving {len(remaining_objects)} unique objects for {category.verbose_name} into the 'unique_records'...")
                            self.db.save_to_db('unique_records', remaining_objects)

                            print(f"Saving {len(objects_to_assign)} objects for {category.verbose_name} into the 'objects'.")
                            self.db.save_to_db('objects', objects_to_assign)
                            print(f'Done for {category.verbose_name}')
                            print("-" * 50)
                            break
                        print(f"No more unique objects for {category.verbose_name}.\nMaking another API call...")
                        offset += limit

            # Save user and assigned objects
            self.save_user_and_objects(user_data, assigned_objects_per_user)

            # Downloading photos and saving to the database
            download_and_save_photos(batch_size=total_goal, source=assigned_objects_per_user)
            print(f"All photos for {user_data['username']} downloaded and saved to the database.")
            print(f"Done for user {user_data['username']}.")
            print("*" * 50)





