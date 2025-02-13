import ast
import asyncio
import csv
import os

import aiofiles
import aiohttp
import asyncpg

from core.settings import DOWNLOAD_DIR, BASE_DIR


async def create_filename(record, counter):
    # Create a unique filename with the domain name and counter
    category = record['category']
    record_id = record['id']
    domain_name = f"{category.lower()}-{record_id}"
    print(f"Filename: {domain_name}-{counter}.jpg")
    return f"{domain_name}-{counter}.jpg"

class Downloader:
    def __init__(
            self, batch_size, source_db=None, source_file=None, source_obj=None, output_db=None, output_storage=None
    ):
        self.batch_size = batch_size
        self.session = None
        self.source_db = source_db
        self.source_file = source_file
        self.source_obj = source_obj
        self.output_db = output_db
        self.pool = None
        self.output_storage = output_storage
        if not self.output_db and not self.output_storage:
            self.output_directory = DOWNLOAD_DIR
            os.makedirs(self.output_directory,exist_ok=True)


    async def init_session(self):
        """Initialize the session inside the event loop."""
        self.session = aiohttp.ClientSession()
        print("Session initialized.")

    async def close_session(self):
        """Close the session inside the event loop."""
        await self.session.close()
        print("Session closed.")

    async def create_pool(self):
        if self.output_db:
            self.pool = await asyncpg.create_pool(**self.output_db)
            print(f"Successfully created a connection pool for the database '{self.output_db['database']}'.")

    async def close_pool(self):
        if self.pool:
            await self.pool.close()
            print("Connection pool to the database closed.")

    async def get_records_from_db(self):
        """
        Asynchronously fetch records from PostgreSQL database using asyncpg.
        """
        # Establish connection using asyncpg
        conn = await asyncpg.connect(**self.source_db)
        query =  "SELECT id, category, photo_URLs FROM objects;"
        # Fetch the records asynchronously
        records = await conn.fetch(query)

        # Close the connection
        await conn.close()
        print(f"Successfully fetched {len(records)} records from the database.")
        return records

    async def get_records_from_csv(self):
        """
        Function to extract records from a CSV file, ensuring photo_URLs is a proper list.
        """
        records = []
        file_path = os.path.join(BASE_DIR, 'data', self.source_file)
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                # Ensure we're accessing the correct column indices
                category = row[1]  # The category should be in the second column (index 1)
                id = row[0]  # The id should be in the first column (index 0)
                photo_urls_str = row[6]  # Assuming the photo URLs are in the 7th column (index 6)

                # Clean up the photo URLs string by removing extra quotes
                photo_urls_str = photo_urls_str.strip()  # Remove leading/trailing spaces
                if photo_urls_str.startswith('"') and photo_urls_str.endswith('"'):
                    # Remove the outer quotes
                    photo_urls_str = photo_urls_str[1:-1]

                # Parse the cleaned-up string into a list using ast.literal_eval
                try:
                    photo_urls = ast.literal_eval(photo_urls_str)
                    if not isinstance(photo_urls, list):
                        photo_urls = []  # Ensure it's a list
                except (ValueError, SyntaxError) as e:
                    print(f"Error parsing photo URLs: {e}")
                    photo_urls = []  # In case parsing fails, use an empty list

                # Create the record and append to the list
                record = {
                    'id': id,
                    'category': category,
                    'photo_URLs': photo_urls
                }
                records.append(record)

        return records

    async def get_objects_from_source(self):
        """
        Asynchronously fetch records from the source object.
        """
        if self.source_db:
            return await self.get_records_from_db()
        elif self.source_file:
            return await self.get_records_from_csv()
        elif self.source_obj:
                return self.source_obj
        else:
            raise ValueError("Invalid source.")


    async def process_record_images(self, record):
        """
        Process and download images for a given record.
        This will be run asynchronously.

        :param record: A dictionary containing 'category', 'unique_id', and 'photo_URLs'.
        """

        photo_urls = record.get('photo_URLs') if record.get('photo_URLs') is not None else record.get('photo_urls')
        # Download each image with a unique counter for the record
        tasks = [self.download_image(record,url, counter) for counter, url in enumerate(photo_urls, start=1)]

        # Gather all the tasks and run them concurrently
        image_data_filename_pairs = await asyncio.gather(*tasks)
        # Prepare records for DB or disk saving
        image_records = [(filename, image_data) for filename, image_data in image_data_filename_pairs if image_data]
        if self.output_db:
            await self.save_to_db(image_records)
        else:
            # Save each image to disk or the database if successfully downloaded
            for filename, image_data in image_records:
                print(f"Saving {filename}")
                if image_data:
                    await self.save_to_disk(filename, image_data)
                else:
                    print(f"No image data found for {filename}...")


    # Create an async function to download an image with a domain name and counter
    async def download_image(self, record, url, counter):
        """
        Download an image with a domain name and counter.

        :param record: A dictionary containing 'category', 'unique_id', and 'photo_URLs'.
        :param url: The URL of the image to download.
        """
        print(f"Processing record: {record}")  # Print the record
        print(f"Trying to download from {url}")
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    image_data = await response.read()
                    print(f"Image data size for {url}: {len(image_data)}")
                    filename = await create_filename(record, counter)
                    return filename, image_data
                else:
                    print(f"Failed to download {url}: Status code {response.status}")
        except Exception as e:
            print(f"{type(e).__name__} occurred downloading {url}: {str(e)}")

        return None, None

    async def save_to_disk(self, image_data, filename):
        """
        Save the downloaded image to the filesystem, a CSV file, or a database,
        depending on the configuration. When saving to the database, the image
        will be stored as binary data (BYTEA).
        """

        save_path = os.path.join(self.output_directory, filename)
        # Save to the file system
        if image_data:
            print(f"Saving to disk: {save_path}")
            async with aiofiles.open(save_path, 'wb') as f:
                await f.write(image_data)
            print(f"Downloaded and saved to disk: {save_path}")
        else:
            print(f"No image data found for {filename}...")

    async def save_to_db(self, image_records):
        """Save image data to the database using connection pool."""
        if not self.pool:
            raise Exception("Connection pool is not initialized.")

        # Acquire a connection from the pool
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                create_table_query = """
                CREATE TABLE IF NOT EXISTS images (
                    id BIGSERIAL PRIMARY KEY,
                    filename VARCHAR(255) UNIQUE,
                    image_data BYTEA
                );
                """
                await conn.execute(create_table_query)

                insert_query = """
                INSERT INTO images (filename, image_data)
                VALUES ($1, $2) ON CONFLICT (filename) DO NOTHING;
                """
                await conn.executemany(insert_query, image_records)
            await conn.close()
            print(f"Successfully inserted/updated {len(image_records)} records into the database.")



    async def run_batch_downloads(self, batch):
        """Process a batch of records concurrently."""
        tasks = [self.process_record_images(record) for record in batch]
        await asyncio.gather(*tasks)


    async def manage_batch_tasks(self):
        records = await self.get_objects_from_source()
        num_of_total_records = len(records)
        print(f"Total records: {num_of_total_records}")

        # Split records into batches
        batch_tasks = []
        for i in range(0, num_of_total_records, self.batch_size):
            batch = records[i:i + self.batch_size]
            print(f"Batch {i // self.batch_size + 1} of {num_of_total_records // self.batch_size + 1} started.")
            batch_tasks.append(self.run_batch_downloads(batch))

        await asyncio.gather(*batch_tasks)


    async def run(self):
        try:
            await self.init_session()
            await self.create_pool()
            await self.manage_batch_tasks()
        finally:
            await self.close_session()
            await self.close_pool()