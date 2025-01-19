# DailyParser Script

## Overview

The `DailyParser` script automates the process of fetching, storing, and assigning objects to users in a PostgreSQL database. The script performs the following tasks:

1. **Fetching Unique Objects**: It checks for unique objects in the `unique_records` table. If objects exist, they are assigned to a user and removed from `unique_records`.
2. **Fetching New Objects from an External API**: If no unique objects are found, the script fetches new objects from an external API (e.g., Avito), checks for duplicates in the `objects` table, and stores them in the `unique_records` table for future use.
3. **User Data Generation**: It generates random user data (username, phone number, email, etc.) using the `Faker` library.
4. **Assigning Objects to Users**: Objects are assigned to users based on the category and object goals for the user.
5. **Error Handling**: In case of failures while fetching or processing data, the script retries fetching objects until a maximum number of failures is reached.

## Features

- **Random User Creation**: Uses `Faker` to generate fake user data such as names, emails, phone numbers, and addresses.
- **Category-Based Object Assignment**: Assigns objects to users from categories (e.g., Real Estate, Vehicles, Electronics).
- **Database Management**: Interacts with PostgreSQL to store and manage users and objects.
- **Retry Mechanism**: Handles retries for failed API calls or missing objects.

## Requirements

- **Python 3.x**
- **Faker**: For generating fake user data.
- **psycopg2**: For interacting with PostgreSQL.
- **Web Scraping API** (e.g., Avito) to fetch new objects.
- A PostgreSQL database with the following tables: `users`, `objects`, and `unique_records`.

## Database Schema

The script interacts with the following tables:

### 1. **users**
This table stores user information.

- `id`: `BIGSERIAL PRIMARY KEY`
- `username`: `TEXT UNIQUE`
- `phone_number`: `VARCHAR(32) UNIQUE`
- `email`: `VARCHAR(64)`
- `first_name`: `VARCHAR(64)`
- `last_name`: `VARCHAR(64)`
- `address`: `VARCHAR(256)`
- `gender`: `CHAR(2)`
- `last_updated`: `TIMESTAMP DEFAULT CURRENT_TIMESTAMP`

### 2. **objects**
This table stores information about the objects being assigned to users.

- `id`: `BIGINT PRIMARY KEY`
- `category`: `VARCHAR(64)`
- `type`: `VARCHAR(64)`
- `title`: `VARCHAR(256)`
- `price`: `TEXT`
- `price_for`: `VARCHAR(64)`
- `location`: `VARCHAR(256)`
- `photo_URLs`: `TEXT[]`
- `source_URL`: `VARCHAR(256)`
- `last_updated`: `TIMESTAMP DEFAULT CURRENT_TIMESTAMP`

### 3. **unique_records**
This table stores unique objects before they are assigned to users.

- `id`: `BIGINT PRIMARY KEY`
- `category`: `VARCHAR(64)`
- `type`: `VARCHAR(64)`
- `title`: `VARCHAR(256)`
- `price`: `TEXT`
- `price_for`: `VARCHAR(64)`
- `location`: `VARCHAR(256)`
- `photo_URLs`: `TEXT[]`
- `source_URL`: `VARCHAR(256)` (Unique)
- `last_updated`: `TIMESTAMP DEFAULT CURRENT_TIMESTAMP`

## Usage

### Step 1: Database Setup

Before running the script, ensure that the PostgreSQL database has the necessary tables: `users`, `objects`, and `unique_records`. You can create these tables using the provided schema definitions.

### Step 2: Install Dependencies

Ensure you have Python 3.x installed and install the required dependencies:

```bash
pip install psycopg2 faker
