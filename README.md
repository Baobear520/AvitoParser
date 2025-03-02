# Daily Parser 


1. [Overview](#overview)

2. [Services](#services)

3. [Structure](#structure)

4. [Features](#features)
5. [Running the Application](#running-the-application)





## Overview

The project is a part of the app that **scores clients' financial credibility using ML and CV.** 
It helps in **collecting data for initial dataset** creation to train ML and CV models 
and aims to **automate the process of generating mock user data** for the app on development stages.
---
## Services

The project provides the following services:

**1. Python 3.12.4 (mock_parser)** - a container for Python code execution

**2. PostgreSQL Database (postgres_db)** - a database for storing user and object data

**3. MinIO S3-Compatible Storage (minio)** - a storage bucket for image data

---
## Structure

The project's functionality lies in **3 main scripts**:
- `main_scripts/initial_dataset_collector.py`: a framework for extracting large datasets from Avito API and saving them in a CSV file or a Postgres database.
- `main_scripts/download_photos.py`: a framework for downloading images of the items fetched from AvitoAPI and saving them locally on a hard drive, in a CSV -file, database, or remotely in a storage bucket.
- `main_scripts/mock_user_data_scraper.py`: a script that automates the process of generating mock user data for a financial credibility scoring app.
It is designed to simulate user activity and generate data for testing or demonstration purposes. 

**Other directories and files in the project:**

`core` contains a collection of utilities and classes that are used across the scripts:

- `core.browsers.py`: classes for managing web browsers
- `core.parsers.py`: classes for parsing web pages
- `core.utilities`: a package that contains a collection of utility classes and functions:
  - `core.utilities.csv.py`: classes for working with CSV files;
  - `core.utilities.enums.py`: classes for defining enums;
  - `core.utilities.minio.py`: classes and functions for working with MinIO storage buckets;
  - `core.utilities.other_functions.py`: a collection of other utility functions

- `core.exceptions.py`: custom exceptions
- `core.downloader.py`: classes for downloading images
- `core.settings.py`: project configuration settings

`database` contains a collection of classes and functions for interacting with databases:
- `database.db.py`: classes for interacting with databases
- `database.db_schema.py`: database schema definitions

`data` - a default folder for storing data files (e.g., CSV files)

`.env` - a file for storing environment variables

`dockerfile` - a file for defining the Docker container used in the project

`docker-compose.yaml` - a file for defining the Docker containers used in the project

`requirements.txt` - a file for defining the Python dependencies used in the project

---
## Features
### mock_user_data_scraper.py
**The script performs the following tasks:**

 - generates a daily number of mock-users with personal credentials;
- assigns property objects of different categories (real estate, cars, electronics, household equipment) to the users with data fetched from Avito API or existing database/CSV file.
- stores users' data in a Postgres database;
- downloads images of each property object for a newly registered user and saves them in a storage bucket.
- passes the image data to CV - models (currently in the works) 

**Flow of the script:**

1. **Database initialization**: It connects to the PostgreSQL database or creates a new one (`DB_NAME`), creates the necessary tables and indexes if they don't exist (`DB_SCHEMA`).
2. **Web driver initialization**: It initializes a web driver for interacting with the source API.
3. **User Data Generation**: It generates random user data (username, phone number, email, etc.) using the `Faker` library.
4. **Fetching Unique Objects**: It checks if there are enough unique objects already available in the `unique_records` database table (table for storing unique objects that are not yet assigned to users) 
for the specified category. If the condition is met, they are assigned to a user, stored in the `objects` table and removed from `unique_records`. If there are no not enough unique objects, you proceed to fetch new objects from the API.
5. **Fetching New Objects**: It fetches new objects from an external API (e.g., Avito), checks for duplicates in the `objects` database table, and stores them in the `unique_records` table for future use.
6. **Assigning Objects to Users**: Objects are assigned to users based on the category and object goals for the user.
7. **Database Management**: It inserts user and object data into the appropriate tables in the database within a single transaction.
8. **Error Handling**: In case of failures while fetching or processing data, the script retries fetching objects until a maximum number of failures is reached.
8. **Image Data Extraction**: It extracts image data for each property object from the current variable (also supports data retrieval from a CSV file or database table).
10. **Image Download**: Images of each property object are downloaded and saved in a storage bucket (Minio) or locally on a hard drive.
11. _**Image Data Passing**: The script passes the image data to CV - models (currently in the works)_

### initial_dataset_collector.py

**The script performs the following tasks:**
- fetches data from an external API (e.g., Avito) for each category(for the current project they are defined in `CategoryType` class in `core.utilities.enums.py`) 
depending on the query parameters (location, last_stamp,total_goal);
- creates a dataset from the fetched data;
- saves it in a CSV file or a database table.

**Flow of the script:**

1. **Web driver initialization**: It initializes a web driver for interacting with the source API.
2. **Data Collection**: It fetches data from an external API (e.g., Avito) for each category with the specified the query parameters.
3. **Data Transformation**: It transforms the fetched data into a pandas DataFrame.
4. **Data Saving**: It saves the DataFrame to a CSV file or a database table.
5. **Retry Mechanism**: Handles retries for failed API calls or missing data.
6. **Error Handling**: In case of failures while fetching (zero-fetches) or processing data, the script retries fetching data until a maximum number of failures is reached.

### download_photos.py

The script uses asyncio and aiohttp to concurrently download images of each property object for a newly registered user and saves them in a Minio storage bucket. The images are stored in a bucket with a unique key (`user_id/object_id/image_counter.jpg`) for each image.

The script also supports the functionality of downloading images from a CSV file or a database table and saving them on a hard drive or a separate database.

---

## Running the Application
### Requirements

To run the application, you need to have Python 3.9 or higher installed on your system.

### Installation

First, clone the repository and switch to the `AvitoParser` directory:

```bash
git clone https://github.com/Baobear520/AvitoParser
cd AvitoParser
```
### Setup
Create a `.env` file and and set up the environment variables for the project:

```bash
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=your_db_host
DB_PORT=your_db_port
MINIO_HOST=your_minio_host
MINIO_ROOT_USER=your_minio_access_key
MINIO_ROOT_PASSWORD=your_minio_secret_key
MINIO_ENDPOINT=your_minio_port
```
Other important but not sensitive settings are defined in `settings.py`


### Running from Docker

To run the application from Docker, you need to have Docker installed on your system.
The `dockerfile` in the root directory of the repository contains the instructions for building the Docker image for the application.
The `docker-compose.yaml` contains the instructions for defining the Docker containers for the application:

- `db` : PostgreSQL database.
- `mock_parser`: Python application.
- `minio`: MinIO storage bucket.

The containers will use the environment variables defined in the `.env` file to run the parser script and connect to the database and MinIO storage bucket.

To start the containers:

```bash
docker compose up -d
```
The application will be available at `http://localhost:8000`

---
### Running Locally

Create a virtual environment, activate it and install the required dependencies from the `requirements.txt` file:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt`
```

Run the `mock_user_data_scraper.py` script from your IDE (e.g., PyCharm) or from the command line:
```bash
python mock_user_data_scraper.py
```

The script's logs will be seen in the terminal _(proper logging configuration is to be added)._


