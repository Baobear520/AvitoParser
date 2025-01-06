import os
from browsers import UndetectedChromeBrowser
from parsers import Parser
from utilities import runtime_counter


@runtime_counter
def main():
        browser = UndetectedChromeBrowser()
        parser = Parser(browser.get_driver)
        
        urls = parser.get_urls(category_id=4,total_goal=5000,limit=300,offset=0)
        data = parser.scrape(urls)
        
        parser.save_to_csv(
            data=data,
            path=os.makedirs('data', exist_ok=True),
            filename='output.csv')

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Manual shutdown...")