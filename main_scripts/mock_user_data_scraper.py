
import random
import time

from core.browsers import ChromeBrowser
from core.db import PostgresDB

from core.parsers import DailyParser
from core.settings import BASE_URL, LIMIT, DB_HOST, DB_USER, DB_PORT, DB_PASSWORD, DB_NAME, DB_SCHEMA, USER_COUNT_RANGE, \
    OBJECT_COUNT_RANGE
from core.utilities import runtime_counter

"""
Existing Unique Records in unique_objects:

You first check if there are unique objects already available in the unique_objects table for the specified category.
If there are enough unique objects that meet the goal for the current user and category:
Assign these objects to the user, with the user_id as a reference to the users table.
After saving the objects, the unique objects should be removed from the unique_objects table.
If there are not enough unique objects, you proceed to fetch new objects from the API.
Fetching New Objects:

If no suitable unique objects exist, you query an external API to fetch new objects.
Once new objects are fetched, filter out those that already exist in the objects table (to prevent duplication).
The newly fetched objects (those not already in objects) are referred to as new_unique_objects.
Handling New Unique Objects:

After filtering the new_unique_objects, you check whether there are enough to meet the goal for the category and the user.
If there are enough new objects:
Subtract the required amount from the new_unique_objects and save the remaining objects back into the unique_objects table.
Then, generate user data, associate the objects with the user by adding user_id to the records, and insert the objects into the objects table.
If the insertion into the objects table is successful, update the user record by adding the user and referencing their user_id in the objects table.
Repeat the Process for All Categories and Users:

This process is repeated for each category.
Then, it is repeated for all users in user_count, ensuring that each user is assigned a unique set of objects.
Key Steps Breakdown:
Check if there are existing unique objects in the unique_objects table:

Query the unique_objects table to check if there are any objects that have not been assigned yet for the given category.
Object Assignment and User Creation:

If unique objects exist for the category, assign the objects to the user by inserting them into the objects table.
The user should be created first (if not already), and their ID should be linked to the objects through the foreign key.
Handle the Case Where No Unique Objects Exist:

If no objects exist for the category in unique_objects, fetch new objects from an external API.
Filter out any objects already present in the objects table to avoid duplication.
If valid new objects are available, assign them to the user and save them in the objects table, and then remove used objects from unique_objects.
Updating the Tables:

After saving the objects, update the user_id in the objects table to reflect the association with the user.
Remove used objects from unique_objects after they are assigned to the user."""


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
    # Create tables
    for table_name, schema in DB_SCHEMA.items():
        print(f"Creating table: {table_name}")
        db.create_table(schema)

    # Create indexes
    for table_name, schema in DB_SCHEMA.items():
        print(f"Creating indexes for table: {table_name}")
        db.create_indexes(table_name, indexes=schema.get('indexes', []))


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
    parser.run(
        driver=driver,
        total_goal=total_goal,
        limit=LIMIT
    )


if __name__ == "__main__":
        try:
            main()
        except KeyboardInterrupt:
            print("Manual shutdown...")
        except Exception as e:
            print(f"Unexpected error occurred: {e}")
