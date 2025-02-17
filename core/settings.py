import os

from dotenv import load_dotenv
from pathlib import Path

from database.db_schema import DB_SCHEMA

""" Settings for the application """

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

# Avito API query parameters (recommended)
BASE_URL = "https://www.avito.ru/web/1/main/items"
LIMIT = 300
MAX_ATTEMPTS = 3

#Daily parser settings
USER_COUNT_RANGE = (5,20) # Number of users to parse
OBJECT_COUNT_RANGE = (1,5) # Number of objects to parse

# Photo download settings
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data", "downloads", "photos") #"data/downloads/photos"

# Postgres settings (set your own)
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT=os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_SCHEMA = DB_SCHEMA

#MinIO settings
MINIO_ROOT_USER = os.getenv('MINIO_ROOT_USER','minio')
MINIO_ROOT_PASSWORD = os.getenv('MINIO_ROOT_PASSWORD','minio123')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
BUCKET_NAME = 'photos'


