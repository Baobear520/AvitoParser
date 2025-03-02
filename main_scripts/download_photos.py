import asyncio

from core.downloader import Downloader
from core.settings import MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_ENDPOINT
from core.utilities.minio import MinioClient


# Main function to run the batch download
def download_and_save_photos(batch_size, source, user_id):

    asyncio.run(
        Downloader(
            batch_size=batch_size,
            source_obj=source,
            output_storage=MinioClient(
            endpoint=MINIO_ENDPOINT,
            root_user=MINIO_ROOT_USER,
            password=MINIO_ROOT_PASSWORD
    ),
            user_id=user_id,

    ).run()
    )


