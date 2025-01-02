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

from exceptions import MaxRetryAttempsReached
from utilities import get_UTC_timestamp, runtime_counter


class Parser:
    """ Class for parsing data from Avito """

    def __init__(self,headless):
        self.headless = headless
        self.driver = self.__setUp__()


    def __setUp__(self):
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")  # Run in headless mode if you don't need a GUI
        chrome_options.add_argument("--disable-gpu")

        # Initialize the Chrome driver
        print("Initializing Chrome driver...")
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


    def __get_json(self,delay, max_attempts=3):

        # Parse the HTML content with BeautifulSoup
        page_source = self.driver.page_source
        #time.sleep(delay)
        self.driver.implicitly_wait(delay)
        
        attempts = 1

        while attempts <= max_attempts:
            try:
                soup = BeautifulSoup(page_source, 'html.parser')

                # Find the <pre> tag that contains the JSON data
                pre_tag = soup.find('pre')

                # Extract the JSON string from the <pre> tag
                json_data = pre_tag.text
                print("Obtained json data.")

                # Convert the JSON string into a Python dictionary
                return json.loads(json_data)
            
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}\nRetrying {attempts}/{max_attempts}...")
                attempts += 1
            except Exception as e:
                print(f"{type(e).__name__} occurred: {e}\nRetrying {attempts}/{max_attempts}...")
                attempts += 1

        raise MaxRetryAttempsReached(msg="Maximum number of retry attempts reached. Skipping...")
        

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


    def run_scraping_loop(self, 
                          base_url, 
                          category_id, 
                          location, 
                          limit,  
                          total_goal,
                          delay):
        
        """ Main scraping loop """
    
        all_properties = []
        last_stamp = get_UTC_timestamp()
        offset = 0


        # Scraping loop
        while len(all_properties) <= total_goal:
            full_url = f'{base_url}forceLocation={location}&lastStamp={last_stamp}&limit={limit}&offset={offset}&categoryId={category_id}'
            
            # Obtaining html
            self.driver.get(full_url)
            print(f"Navigated to {self.driver.current_url}")

            try:
                json_data = self.__get_json(delay)
                parsed_data = self.__parse_data(json_data)
                all_properties.extend(parsed_data)

            except MaxRetryAttempsReached as e:
                print(e)
        
            except Exception as e:
                print(f"{type(e).__name__} occurred: {e}")
            
            offset += limit

            # Waiting before the next request
            time.sleep(delay)
    
        self.driver.quit()
        # Print all fetched properties
        print(f"Total properties fetched: {len(all_properties)}")
        return all_properties


@runtime_counter
def main():
    parser = Parser(headless=False)
    data = parser.run_scraping_loop(
        base_url='https://www.avito.ru/web/1/main/items?',
        location=False,
        category_id=4,
        total_goal=1000,
        limit=300,
        delay=5
    )
    parser.save_to_csv(data=data,path='data',filename='output.csv')


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Manual shutdown...")
    
    


  




