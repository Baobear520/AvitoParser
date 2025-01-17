import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException

class ChromeBrowser:
    """ A class for initializing base Chrome webdriver"""

    def __init__(self, headless=False, disable_images=True, timeout=30, proxy=None):
        self.headless = headless
        self.disable_images = disable_images
        self.timeout = timeout
        self.proxy = proxy


    def _set_options(self,options=None):
        """Set Chrome options."""
        options = options or Options()
        options.add_argument("--disable-gpu")

        if self.headless:
            options.add_argument("--headless")  # Run in headless mode if you don't need a GUI
        
        if self.disable_images:
            options.add_argument('--blink-settings=imagesEnabled=false') #disable images
        
        if self.proxy:
            options.add_argument(f'--proxy-server={self.proxy}')
        
        return options

    def get_driver(self):
        # Initialize the Chrome driver
        print("Initializing Chrome driver...")
        try:
            options = self._set_options()
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()), 
                options=options
                )
            driver.set_page_load_timeout(self.timeout)
            print(f"Driver object {driver} has been initialized.")
            return driver
        
        except TimeoutException as e:
            print(f"Couldn't load the page source.")
            raise e
        except Exception as e:
            print(f"{type(e).__name__} occured during driver initialization: {e}")
            raise e


class UndetectedChromeBrowser(ChromeBrowser):
    """ A class for initializing undetected Chrome webdriver"""


    def get_driver(self):

        # Initialize the Chrome driver
        print("Initializing Undetected Chrome driver...")
        try:
            options = self._set_options(options=uc.ChromeOptions())
            driver = uc.Chrome(options)
            driver.set_page_load_timeout(self.timeout)
            print(f"Driver object {driver} has been initialized.")
            return driver
        
        except TimeoutException as e:
            print(f"Couldn't load the page source.")
            raise e
        except Exception as e:
            print(f"{type(e).__name__} occured during driver initialization: {e}")
            raise e
    

