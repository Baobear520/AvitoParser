
import random
import time

from core.browsers import UndetectedChromeBrowser, ChromeBrowser
from core.db import PostgresDB, DB_HOST, DB_USER, DB_PORT, DB_PASSWORD, DB_NAME, USER_SCHEMA, \
    UNIQUE_RECORD_SCHEMA, OBJECT_SCHEMA
from core.enums import CategoryType
from core.parsers import DailyParser
from core.utilities import runtime_counter


@runtime_counter
def main():
    # PostgresDB instance
    db = PostgresDB(
        host=DB_HOST,
        user=DB_USER,
        port=DB_PORT,
        password=DB_PASSWORD,
        db_name=DB_NAME
    )
    # Initialize the database
    # Create 'objects' table
    unique_constraints = ['source_URL']
    db.create_table("objects", OBJECT_SCHEMA, unique_constraints=unique_constraints)
    #
    # # Create 'unique_records' table
    unique_constraints = ['source_URL']
    db.create_table("unique_records", UNIQUE_RECORD_SCHEMA, unique_constraints=unique_constraints)
    #
    # # Create 'users' table
    foreign_keys_users = [("property", "objects", "id")]
    unique_constraints = ['username','phone_number']
    db.create_table("users", USER_SCHEMA, foreign_keys=foreign_keys_users, unique_constraints=unique_constraints)
    print("Database initialized.")
    print("*" * 50)

    browser = ChromeBrowser(headless=True)
    driver = browser.get_driver()
    base_url = "https://www.avito.ru/web/1/main/items"

    # App configuration
    user_count = random.randint(5, 20)
    print(f"Total number of today's users: {user_count}")
    total_goal = random.randint(1, 5)
    print(f"Total number of objects per category per user: {total_goal}")
    print("*" * 50)

    # Initialize and run the daily parser
    parser = DailyParser(
        db=db,
        browser=browser,
        base_url=base_url,
        user_count=user_count
    )

    print("Starting the daily parser...")
    print("*" * 50)
    parser.run(
        driver=driver,
        total_goal=total_goal,
        limit=300
    )

if __name__ == "__main__":
    if __name__ == "__main__":
        while True:
            try:
                main()
                time.sleep(60)
                print("Restarting the Daily parser...")
            except KeyboardInterrupt:
                print("Manual shutdown...")
                break
            except Exception as e:
                print(f"Unexpected error occurred: {e}")
                break