import os

from dotenv import load_dotenv
from pathlib import Path

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
BATCH_SIZE = 10
DOWNLOAD_DIR = "data/downloads/photos"

# Postgres settings (set your own)
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT=os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Table schemas
OBJECT_SCHEMA={
    "id": "BIGINT PRIMARY KEY",
    "category": "VARCHAR(64)",
    "type": "VARCHAR(64)",
    "title": "VARCHAR(256)",
    "price": "TEXT",
    "price_for": "VARCHAR(64)",
    "location": "VARCHAR(256)",
    "photo_URLs": "TEXT[]",
    "source_URL": "VARCHAR(256)",
    "last_updated": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    'user_id': 'BIGINT'
}
OBJECT_CONSTRAINTS=['source_URL']

UNIQUE_RECORD_SCHEMA={
    "id": "BIGINT PRIMARY KEY",
    "category": "VARCHAR(64)",
    "type": "VARCHAR(64)",
    "title": "VARCHAR(256)",
    "price": "TEXT",
    "price_for": "VARCHAR(64)",
    "location": "VARCHAR(256)",
    "photo_URLs": "TEXT[]",
    "source_URL": "VARCHAR(256) UNIQUE",
    "last_updated": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}
UNIQUE_RECORDS_CONSTRAINTS=['source_URL']

USER_SCHEMA={
    "id": "BIGSERIAL PRIMARY KEY",
    "username": "TEXT UNIQUE",
    "phone_number": "VARCHAR(32) UNIQUE",
    "email": "VARCHAR(64)",
    "first_name": "VARCHAR(64)",
    "last_name": "VARCHAR(64)",
    "address": "VARCHAR(256)",
    "gender": "CHAR(2)",
    "property": "BIGINT",
    "last_updated": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
}
USER_CONSTRAINTS=['username', 'phone_number']
USER_FK=[('property', 'objects', 'id')]


print(DB_PASSWORD)