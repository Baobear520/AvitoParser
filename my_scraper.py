
import time
from bs4 import BeautifulSoup
from datetime import datetime
import cloudscraper

# s = cloudscraper.create_scraper(delay=10, browser={'custom': 'ScraperBot/1.0'})

# #url = 'https://www.avito.ru/web/1/main/items'
# headers = {
#     'authority':'m.avito.ru',
#     'accept':'application/json',
#     'accept-encoding':'gzip, deflate, br, zstd',
#     'accept-language':'en-US,en;q=0.9,ru;q=0.8',
#     #'cookie':'srv_id=WqDNttlNW8IGZmHM.JfS_a598RCa1jpknHYo1fr9N8cbEf9lDCOSp1UeQSRV63w-G30f6nRvlx7q4B3Ese-Mg.gUy21qQtmB-c5GDBSnXoeKt1PfhnzEQ3p5m592jUTh4=.web; u=32tdzcz9.1bz5uvc.cuju92izaz80; v=1734934133; luri=all; buyer_location_id=621540; sx=H4sIAAAAAAAC%2FwTAPQ6FIAwH8Lv85ze0fBQeR3BycqdSYuLgpjGEu%2Fsb0KxZmfSfZM%2BiYtatU3PElVoIFWXgRsHC6dnkMn0PXePZ8YOhcPKRHEXv5%2FwCAAD%2F%2F1LSoBdMAAAA; dfp_group=41; abp=0; cookie_consent_shown=1; _gcl_au=1.1.852425848.1734934146; gMltIuegZN2COuSe=EOFGWsm50bhh17prLqaIgdir1V0kgrvN; _ga_M29JC28873=GS1.1.1734934146.1.0.1734934146.60.0.0; _ga=GA1.1.108191496.1734934146; __ai_fp_uuid=20350f5354da29e1%3A1; _ym_uid=1734934147628871316; _ym_d=1734934147; _ym_isad=2; _ym_visorc=b; tmr_lvid=01b0ec32aa3d1c8de7dddcede21751d1; tmr_lvidTS=1734934149048; uxs_uid=6c9b3940-c0f4-11ef-ba18-5381b66ccaf2; buyer_from_page=vertical; adrfpip=S37wanaPD69T; adrfpip=S37wanaPD69T; tmr_detect=0%7C1734934151318',
#     'referer':'https://www.avito.ru/all/nedvizhimost',
#     'sec-fetch-site': 'none',
#     'sec-fetch-mode': 'navigate',
#     'sec-fetch-user': '?1',
#     'sec-fetch-dest': 'document',
#     'user-agent':'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36'
# }
# cookies = {
    
#     'f': '5.32e32548b6f3e9784b5abdd419952845a68643d4d8df96e9a68643d4d8df96e9a68643d4d8df96e9a68643d4d8df96e94f9572e6986d0c624f9572e6986d0c624f9572e6986d0c62ba029cd346349f36c1e8912fd5a48d02c1e8912fd5a48d0246b8ae4e81acb9fa143114829cf33ca746b8ae4e81acb9fa46b8ae4e81acb9fae992ad2cc54b8aa8b175a5db148b56e9bcc8809df8ce07f640e3fb81381f359178ba5f931b08c66a59b49948619279110df103df0c26013a2ebf3cb6fd35a0ac91e52da22a560f550df103df0c26013a7b0d53c7afc06d0bba0ac8037e2b74f92da10fb74cac1eab71e7cb57bbcb8e0f71e7cb57bbcb8e0f71e7cb57bbcb8e0f0df103df0c26013a037e1fbb3ea05095de87ad3b397f946b4c41e97fe93686adbf5c86bc0685a4ff42a08f76f4956e8502c730c0109b9fbb88742379a681fe552709b26a275ecd440e28148569569b79099f064490bd264aaffcb3ba7c814b452ebf3cb6fd35a0ac0df103df0c26013a28a353c4323c7a3a140a384acbddd748ec69374753ddd6b03de19da9ed218fe23de19da9ed218fe2ddb881eef125a8703b2a42e40573ac3c8edd6a0f40cbfd87da3d420d6cca468c',
#     'ft': '"lXh+9XB6Da+f+A03Jx6nMRILS6YV95XEpyuIlTPmZ1P6+eMfHKfBUtIwTF2CVRSTgiS9qEq4zI39C2Yr8L1oB4DwFXnxjTHs1RGOlLcQuA7c0bNfgOYNvgGHTc29VBWh3yWlLb1DikbE22G4WmDQ1mNDxQCmB698vyunxx4tZIT+Z5dxe8FDsGGdDXxtS/wt"',
 
# }
# params = {
#     'forceLocation': False,
#     'locationId': 653040,
#     'lastStamp': 1683748131,
#     'limit': 30,
#     'offset': 89,
#     'categoryId': 4
# }
# r = s.get(url, headers=headers,params=params,cookies=cookies)
# print(r)

import json
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


def chrome_setup():
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
    return driver



def get_json(driver):

    # Parse the HTML content with BeautifulSoup
    page_source = driver.page_source
    time.sleep(5)
    #driver.implicitly_wait(5)

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
        
def parse_data(data):
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
                'url': None
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


def print_objects(objects): 
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



@runtime_counter
def main():
    base_url = 'https://www.avito.ru/web/1/main/items?'

    # Params
    location = False
    #location_id = 653040
    last_stamp = get_timestamp(datetime.now())
    limit = 10
    offset = 0
    category_id = 4

    # Loop variables
    all_properties = []
    total_goal = 10
    step = limit

    #Initialize Chrome driver
    driver = chrome_setup()
    print(f"Driver has been initialized.")

    try:
        # Scraping loop
        for _ in range(0,total_goal,step):
            full_url = f'{base_url}forceLocation={location}&lastStamp={last_stamp}&limit={limit}&offset={offset}&categoryId={category_id}'
            
            # Obtaining html
            driver.get(full_url)
            print(f"Navigated to {driver.current_url}")

            json_data = get_json(driver)
            if json_data:
                parsed_data = parse_data(json_data)
                print_objects(parsed_data)
                all_properties.extend(parsed_data)
            offset += step
            if total_goal - offset < step:
                step = total_goal - offset
            # Waiting before the next request
            time.sleep(5)

    except Exception as e:
        print(f"{type(e).__name__} occurred: {e}")
    finally:
        driver.quit()
        # Print all fetched properties
        print(f"Total properties fetched: {len(all_properties)}")
    




        



if __name__ == "__main__":
    main()
    
    


  




