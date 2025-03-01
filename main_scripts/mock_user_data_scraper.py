
import random

from core.browsers import ChromeBrowser
from core.utilities.minio import MinioClient
from database.db import PostgresDB, DailyParserDB

from core.parsers import DailyParser, generate_user_data
from core.settings import BASE_URL, LIMIT, DB_HOST, DB_USER, DB_PORT, DB_PASSWORD, DB_NAME, DB_SCHEMA, USER_COUNT_RANGE, \
    OBJECT_COUNT_RANGE, MINIO_ENDPOINT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
from core.utilities.other_functions import runtime_counter
from main_scripts.download_photos import download_and_save_photos
from main_scripts.refactoring_mock_users_script import DailyParserTest


@runtime_counter
def main():
    # PostgresDB instance
    db = DailyParserDB(
        host=DB_HOST,
        user=DB_USER,
        port=DB_PORT,
        password=DB_PASSWORD,
        db_name=DB_NAME,
        db_schema=DB_SCHEMA
    )
    # Initialize the database
    # Create tables
    for table_name, schema in DB_SCHEMA.items():
        print(f"Creating table: {table_name}")
        db.create_table(schema)

    # Create indexes
    for table_name, schema in DB_SCHEMA.items():
        print(f"Creating indexes for table: {table_name}")
        db.create_indexes(schema)

    print("Database initialized.")
    print("*" * 50)

    browser = ChromeBrowser(headless=True)
    driver = browser.get_driver()

    # App configuration
    user_count = random.randint(*USER_COUNT_RANGE)
    print(f"Total number of today's users: {user_count}")
    total_goal = random.randint(*OBJECT_COUNT_RANGE)
    print(f"Total number of objects per category per user: {total_goal}")
    print("*" * 50)

    # Initialize and run the daily parser
    parser = DailyParser(
        db=db,
        browser=browser,
        base_url=BASE_URL,
        user_count=user_count
    )
    print("Starting the daily parser...")
    print("*" * 50)
    for _ in range(user_count):
        user_data = generate_user_data()
        username = user_data['username']
        print(f"Generating user {username}...")

        assigned_objects = parser.run(
            driver=driver,
            total_goal=total_goal,
            limit=LIMIT
        )
        user_id = db.save_user_and_objects(user_data, assigned_objects)
        print(f"Done with object assignment for user {username}.")
        print("*" * 50)
        download_and_save_photos(batch_size=total_goal,source=assigned_objects,user_id=user_id)
        print(f"Done for user {username}.")
        print("*" * 50)

if __name__ == "__main__":
        try:
            main()
        except KeyboardInterrupt:
            print("Manual shutdown...")
        except Exception as e:
            print(f"Unexpected error occurred: {e}")
