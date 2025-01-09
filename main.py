import os
from core.browsers import UndetectedChromeBrowser
from core.enums import CategoryType
from core.parsers import Parser
from core.utilities import runtime_counter
from core.db import save_to_postgres

@runtime_counter
def main():
        browser = UndetectedChromeBrowser()
        parser = Parser(browser.get_driver)

        category = CategoryType.REAL_ESTATE
        urls = parser.get_urls(category=category,total_goal=1000,limit=300,offset=0)
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