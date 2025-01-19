
import pandas as pd
import psycopg2

from core.browsers import UndetectedChromeBrowser
from core.parsers import BaseParser
from core.utilities import runtime_counter, PandasHelper
from core.db import PostgresDB
from core.settings import DB_HOST, DB_USER, DB_PORT, DB_PASSWORD, DB_NAME, OBJECT_SCHEMA, LIMIT, BASE_URL


@runtime_counter
def main():
    # Initialize the database
    try:
        # db = PostgresDB(DB_HOST, DB_USER, DB_PORT, DB_PASSWORD, DB_NAME)
        # db.create_table(DB_TABLE_NAME,PRODUCT_SCHEMA)

        browser = UndetectedChromeBrowser()
        parser = BaseParser(browser, base_url=BASE_URL)
        data = parser.run(driver=browser.get_driver(), total_goal=1200, limit=LIMIT)


        #db.save_to_db(DB_TABLE_NAME, data)

        df = pd.DataFrame(data)
        helper = PandasHelper()
        helper.save_data_to_csv_file(
            df, 'experiment1.csv',create_new_file=False)


    except psycopg2.OperationalError as e:
        print(f"Database connection error: {e}")
    except Exception as e:
        print(f"{type(e).__name__}: {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Manual shutdown...")