import os
from browsers import UndetectedChromeBrowser
from parsers import Parser
from utilities import runtime_counter
from db import save_to_postgres

@runtime_counter
def main():
        browser = UndetectedChromeBrowser()
        parser = Parser(browser.get_driver)
        
        urls = parser.get_urls(category_id=4,total_goal=1000,limit=300,offset=0)
        data = parser.scrape(urls)
        
        os.makedirs('data', exist_ok=True)
        parser.save_to_csv(
            data=data,
            path='data',
            filename='output2.csv')

        save_to_postgres(data)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Manual shutdown...")