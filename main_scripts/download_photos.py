import ast
import csv
import os
import threading

import aiohttp
import aiofiles
import asyncio
from concurrent.futures import ThreadPoolExecutor

from core.settings import BATCH_SIZE, DOWNLOAD_DIR, BASE_DIR
from core.utilities import runtime_counter


# Create an async function to download an image with a domain name and counter
async def download_image(record, url, download_dir, counter):
    """
    Download an image with a domain name and counter.

    :param record: A dictionary containing 'category', 'unique_id', and 'photo_URLs'.
    :param url: The URL of the image to download.
    :param download_dir: The directory where the images should be saved.
    :param counter: The counter for the image (e.g., car-21221-1.jpg, car-21221-2.jpg).
    """
    print(f"Processing record: {record}")  # Print the record
    category = record['category']
    id = record['id']
    domain_name = f"{category.lower()}-{id}"
    print(f"Domain name: {domain_name}") # Domain based on category and unique_id
    print(f"Trying to download from {url}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    image_data = await response.read()
                    print(f"Image data size for {url}: {len(image_data)}")

                    # Create a unique filename with the domain name and counter
                    image_filename = f"{domain_name}-{counter}.jpg"
                    save_path = os.path.join(download_dir, image_filename)

                    # Save the image to the filesystem
                    async with aiofiles.open(save_path, 'wb') as f:
                        await f.write(image_data)
                    print(f"Downloaded: {save_path}")
                else:
                    print(f"Failed to download {url}: Status code {response.status}")
    except Exception as e:
        print(f"Error downloading {url}: {str(e)}")


# Function to process a list of photo URLs for a single record
async def process_record_images(record, download_dir):
    """
    Process and download images for a given record.
    This will be run asynchronously.

    :param record: A dictionary containing 'category', 'unique_id', and 'photo_URLs'.
    :param download_dir: Directory to save the images.
    """
    photo_urls = record['photo_URLs']
    tasks = []
    print(f"Splitting images for {record['id']}: {photo_urls}")  # Print the list of photo URLs for the record (photo_urls)
    # Download each image with a unique counter for the record
    for counter, url in enumerate(photo_urls, start=1):
        print(f"Downloading image: {url}")
        task = download_image(record, url, download_dir, counter)
        tasks.append(task)

    # Run the download tasks asynchronously
    await asyncio.gather(*tasks)


# Function to get records from a database or CSV
def get_records_from_csv(file_path):
    """
    Function to extract records from a CSV file, ensuring photo_URLs is a proper list.

    :param file_path: Path to the CSV file.
    :return: A list of records with 'category', 'unique_id', and 'photo_URLs'.
    """
    records = []
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            # Ensure we're accessing the correct column indices
            category = row[1]  # The category should be in the second column (index 1)
            id = row[0]         # The id should be in the first column (index 0)
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


# Function to download images for all records using threading and batching
def download_images_in_batches(file_path, download_dir, batch_size):
    """
    Download images in batches using threading to improve performance.

    :param file_path: Path to the CSV or DB file that contains records.
    :param download_dir: Directory to save the images.
    :param batch_size: Number of records to process per batch.
    """
    records = get_records_from_csv(file_path)
    total_records = len(records)

    # Executor to run the download tasks in batches
    with ThreadPoolExecutor() as executor:
        # Divide records into batches
        for i in range(0, total_records, batch_size):
            batch = records[i:i + batch_size]
            executor.submit(run_batch_downloads, batch, download_dir)
            print(f"{threading.current_thread().name}: Batch {i // batch_size + 1} of {total_records // batch_size + 1} completed.")


# Function to handle downloading images for each batch of records
def run_batch_downloads(batch, download_dir):
    """
    Process a batch of records and download the associated images.

    :param batch: List of records to process.
    :param download_dir: Directory to save the images.
    """
    loop = asyncio.new_event_loop()  # Create a new event loop for each batch
    asyncio.set_event_loop(loop)  # Set the event loop
    tasks = []

    # Create a task for each record in the batch to download its images
    for record in batch:
        tasks.append(process_record_images(record, download_dir))
        print(f"{threading.current_thread().name}: Adding task for the record {record['id']}")

    loop.run_until_complete(asyncio.gather(*tasks))  # Run the tasks asynchronously

@runtime_counter
def main():
    # Example usage
    file_path = os.path.join(BASE_DIR, "data", "experiments", "Электроника.csv") # Replace with your file path
    download_dir = os.path.join(BASE_DIR, DOWNLOAD_DIR)  # Replace with your DOWNLOAD_DIR  # Replace with your download directory
    os.makedirs(download_dir, exist_ok=True)
    batch_size = BATCH_SIZE  # Number of records to process per batch

    download_images_in_batches(file_path, download_dir, batch_size)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Manual shutdown...")
