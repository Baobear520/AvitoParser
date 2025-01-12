import os

import pandas as pd
import psycopg2

from core.browsers import UndetectedChromeBrowser
from core.enums import CategoryType
from core.parsers import Parser
from core.utilities import runtime_counter, PandasHelper
from core.db import PostgresDB, DB_HOST, DB_USER, DB_PORT, DB_PASSWORD, DB_NAME, DB_TABLE_NAME, PRODUCT_SCHEMA


@runtime_counter
def main():
    # Initialize the database
    try:
        db = PostgresDB(DB_HOST, DB_USER, DB_PORT, DB_PASSWORD, DB_NAME)
        db.create_table(DB_TABLE_NAME,PRODUCT_SCHEMA)

        browser = UndetectedChromeBrowser()
        parser = Parser(browser.get_driver)

        category = CategoryType.ELECTRONICS
        urls = parser.get_urls(category=category,total_goal=300,limit=300,offset=0)
        data = parser.scrape(urls)

        db.save_to_db(DB_TABLE_NAME, data)

        df = pd.DataFrame(data)
        helper = PandasHelper()
        helper.save_data_to_csv_file(
            df, f'{category.verbose_name}.csv')


    except psycopg2.OperationalError as e:
        print(f"Database connection error: {e}")
    except Exception as e:
        print(f"{type(e).__name__}: {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Manual shutdown...")