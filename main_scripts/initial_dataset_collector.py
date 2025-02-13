
import pandas as pd
import psycopg2

from core.browsers import UndetectedChromeBrowser
from core.parsers import BaseParser
from core.utilities.csv import PandasHelper
from core.utilities.other_functions import runtime_counter
from core.settings import  LIMIT, BASE_URL


@runtime_counter
def main():
    try:
        browser = UndetectedChromeBrowser()
        parser = BaseParser(browser, base_url=BASE_URL)
        data = parser.run(driver=browser.get_driver(), total_goal=1200, limit=LIMIT)

        # Save data to a CSV file
        output_filename = "experiment1.csv"
        df = pd.DataFrame(data)
        helper = PandasHelper()
        helper.save_data_to_csv_file(
            df, output_filename,create_new_file=False)


    except psycopg2.OperationalError as e:
        print(f"Database connection error: {e}")
    except Exception as e:
        print(f"{type(e).__name__}: {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Manual shutdown...")