import asyncio

from core.downloader import Downloader
from core.settings import DB_HOST, DB_USER, DB_PASSWORD


# Main function to run the batch download
def download_and_save_photos(batch_size, source):
    output_db = {
        'host': DB_HOST,
        'user': DB_USER,
        'port': '5432',
        'password': DB_PASSWORD,
        'database': 'photos'
    }
    asyncio.run(
        Downloader(
            batch_size=batch_size,
            source_obj=source,
            output_db=output_db
    ).run()
    )


