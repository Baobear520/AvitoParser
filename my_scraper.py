from datetime import datetime
import json
import os
import time
import csv
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth

from utilities import get_timestamp, runtime_counter


class Parser:
    """ Class for parsing data from Avito """

    def __init__(
            self, 
            base_url, 
            category_id, 
            total_goal,
            location=False, 
            limit=300, 
            offset=0
            ):  
        self.base_url = base_url
        self.category_id = category_id
        self.location = location
        self.limit = limit
        self.offset = offset
        self.total_goal = total_goal
        self.all_properties = []
        self.last_stamp = get_timestamp(datetime.now())
        self.driver = self.__setUp__()


    def __setUp__(self):
        chrome_options = Options()
        #chrome_options.add_argument("--headless")  # Run in headless mode if you don't need a GUI
        chrome_options.add_argument("--disable-gpu")

        # Initialize the Chrome driver
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), 
            options=chrome_options
            )
        # Selenium Stealth settings
        stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
        )
        print(f"Driver has been initialized.")
        return driver


    def __get_json(self,delay):

        # Parse the HTML content with BeautifulSoup

        page_source = self.driver.page_source
        time.sleep(delay)
        #driver.implicitly_wait(delay)

        soup = BeautifulSoup(page_source, 'html.parser')

        # Find the <pre> tag that contains the JSON data
        pre_tag = soup.find('pre')

        if pre_tag:
            # Extract the JSON string from the <pre> tag
            json_data = pre_tag.text
            print("Obtained json data.")

            try:
                # Convert the JSON string into a Python dictionary
                return json.loads(json_data)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
        else:
            print("No <pre> tag found containing the JSON data.")
        

    def __parse_data(self,data):
        property_objects = []

        # Loop through the items and extract the relevant data
        for item in data.get('items', []):
            try:
                property = {
                    'id': None,
                    'category': None,
                    'title': None,
                    'price': None,
                    'price_for': None,
                    'location': None,
                    'photos': None,
                    'URL': None
                }
                # Extracting the title, location, price, and images for each item
                property['id'] = item.get('id')
                property['title'] = item.get('title', 'N/A')
                property['category'] = item.get('category').get('slug')
                property['location'] = item.get('location', {}).get('name', 'N/A')
                property['URL'] = item.get('urlPath', '')
                
                price_for = item.get('priceDetailed',{}).get('postfix','')
                property['price'] = item.get('priceDetailed', {}).get('string', 'N/A')
                property['price_for'] = price_for if price_for else 'на продажу'

                
                # Extracting image URLs
                property['photos'] = [image.get('864x864', '') for image in item.get('images', [])]
                # Add property to the list of all properties
                property_objects.append(property)

            except Exception as e:
                print(f"{type(e).__name__} occured: {e}")
        
        print(f"Total: {len(property_objects)} properties.")
        return property_objects


    def __print_objects(self,objects): 
        for obj in objects:
            # Output the extracted data
            print(f"ID: {obj['id']}")
            print(f"Title: {obj['title']}")
            print(f"Price: {obj['price']}")
            print(f"Price for: {obj['price_for']}")
            print(f"Location: {obj['location']}")
            print("Photos:")
            for url in obj['photos']:
                print(f"  - {url}")
            print("-" * 40)  # Divider between items


    def save_to_csv(self, data, path, filename):
        # Specify the fieldnames (column names)
        fieldnames = data[0].keys()  # Extract the keys from the first dictionary
        path_to_file = os.path.join(path,filename)
        # Open the CSV file for writing
        with open(path_to_file, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            
            # Write the header (column names)
            writer.writeheader()
            
            # Write the rows (data)
            writer.writerows(data)

        print("CSV file has been written.")

    

    def run_scraping_loop(self, delay):
        """ Main scraping loop """
        try:
            # Scraping loop
            for _ in range(0,self.total_goal,self.limit):
                full_url = f'{self.base_url}forceLocation={self.location}&lastStamp={self.last_stamp}&limit={self.limit}&offset={self.offset}&categoryId={self.category_id}'
                
                # Obtaining html
                self.driver.get(full_url)
                print(f"Navigated to {self.driver.current_url}")

                json_data = self.__get_json__(delay)
                if json_data:
                    parsed_data = self.__parse_data__(json_data)
                    self.all_properties.extend(parsed_data)

                self.offset += self.limit
                if self.total_goal - self.offset < self.limit:
                    self.limit = self.total_goal - self.offset
                # Waiting before the next request
                time.sleep(delay)

        except Exception as e:
            print(f"{type(e).__name__} occurred: {e}")
        finally:
            self.driver.quit()
            # Print all fetched properties
            print(f"Total properties fetched: {len(self.all_properties)}")
            return self.all_properties


@runtime_counter
def main():
    parser = Parser(
        base_url = 'https://www.avito.ru/web/1/main/items?',
        category_id=4,
        total_goal=10000,
        limit=300
    )
    
    data = set(parser.run_scraping_loop(delay=3))
    parser.save_to_csv(data,path='data',filename='output.csv')


if __name__ == "__main__":
    main()
    
    


  




