import psycopg2
from psycopg2.extras import execute_values

DB_HOST = 'localhost'
DB_PORT='5432'
DB_USER = 'aldmikon'
DB_PASSWORD = '123'
DB_NAME = 'test_app'


OBJECT_SCHEMA = {
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
UNIQUE_RECORD_SCHEMA = {
    "id": "BIGINT PRIMARY KEY",
    "category": "VARCHAR(64)",
    "type": "VARCHAR(64)",
    "title": "VARCHAR(256)",
    "price": "TEXT",
    "price_for": "VARCHAR(64)",
    "location": "VARCHAR(256)",
    "photo_URLs": "TEXT[]",
    "source_URL": "VARCHAR(256) UNIQUE",  # Ensuring source_URL is unique
    "last_updated": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}
USER_SCHEMA = {
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


class PostgresDB:
    """ A class for Postgres database operations. """

    def __init__(self, host, user, port, password, db_name):
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.db_name = db_name
        self.conn = self.__connect()

    def __connect(self):
        """Establish a database connection."""
        return psycopg2.connect(
            database=self.db_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    def create_table(self, table_name, schema, foreign_keys=None, unique_constraints=None, other_constraints=None):
        """
        Creates a table based on the provided schema, with optional foreign keys and constraints.

        :param table_name: Name of the table.
        :param schema: Dictionary defining column names and types.
        :param foreign_keys: List of tuples defining foreign key relationships (column_name, referenced_table, referenced_column).
        :param unique_constraints: List of column names that should be unique.
        :param other_constraints: List of other constraints (e.g., CHECK constraints, NOT NULL).
        """
        # Define columns and their types
        columns = ", ".join([f"{col_name} {col_type}" for col_name, col_type in schema.items()])

        # Define foreign keys (if any)
        if foreign_keys:
            foreign_keys_clause = ", ".join([
                f"FOREIGN KEY ({col_name}) REFERENCES {ref_table}({ref_column})"
                for col_name, ref_table, ref_column in foreign_keys
            ])
            foreign_keys_clause = f", {foreign_keys_clause}" if foreign_keys_clause else ""
        else:
            foreign_keys_clause = ""

        # Define unique constraints (if any)
        if unique_constraints:
            unique_constraints_clause = f", UNIQUE ({', '.join(unique_constraints)})"
        else:
            unique_constraints_clause = ""

        # Define other constraints (if any)
        if other_constraints:
            other_constraints_clause = f", {', '.join(other_constraints)}"
        else:
            other_constraints_clause = ""

        # Combine all clauses
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
            {foreign_keys_clause}
            {unique_constraints_clause}
            {other_constraints_clause}
        );
        """

        # Execute the query to create the table
        try:
            with self.__connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_query)
                    print(f"Table '{table_name}' created successfully (if not existed).")
        except psycopg2.Error as e:
            print(f"Error during table creation: {e}")
        except Exception as e:
            print(f"{type(e).__name__} occurred during table creation: {e}")


    def save_to_db(self, table_name, data):
        """
        Inserts or updates data in the specified table.
        :param table_name: Name of the table.
        :param data: List of dictionaries representing rows to be inserted/updated.
        """
        if not data:
            print("No data to save.")
            return

        # Extract columns and values from the first row of data
        columns = data[0].keys()
        values = [[row[col] for col in columns] for row in data]

        insert_query = f"""
                            INSERT INTO {table_name} ({', '.join(columns)})
                            VALUES %s
                            ON CONFLICT (id) DO UPDATE SET
                            {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])};
                        """

        try:
            with self.__connect() as conn:
                with conn.cursor() as cursor:
                    execute_values(cursor, insert_query, values)
                    print(f"Inserted/Updated {len(data)} rows into '{table_name}'.")
        except psycopg2.Error as e:
            print(f"Error during data insertion: {e}")
        except Exception as e:
            print(f"{type(e).__name__} occurred during insertion: {e}")

    def check_for_unique_in_db(self, table_name, category_name):
        """Fetch unique objects from the 'unique_records' table for a specific category."""

        # Validate table name
        valid_tables = ['unique_records', 'objects']  # List of valid table names
        if table_name not in valid_tables:
            raise ValueError(f"Invalid table name: {table_name}")

        try:
            with self.conn as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, category, type, owner, title, price, price_for, location, photo_URLs, source_URL, last_updated
                        FROM {0}
                        WHERE category = %s;
                    """.format(table_name), (category_name,))
                    rows = cursor.fetchall()

                    # Ensure that rows are properly fetched
                    if not rows:
                        print(f"No records found in table '{table_name}' for category '{category_name}'.")

                    # Map rows into dictionaries correctly
                    unique_objects = [
                        {
                            'id': row[0],
                            'category': row[1],
                            'type': row[2],
                            'owner': row[3],
                            'title': row[4],
                            'price': row[5],
                            'price_for': row[6],
                            'location': row[7],
                            'photo_URLs': row[8],
                            'source_URL': row[9],
                            'last_updated': row[10]
                        }
                        for row in rows
                    ]

                    # Debugging to check the structure
                    print(f"Fetched {len(unique_objects)} unique objects from the database.")
                    return unique_objects

        except Exception as e:
            print(f"Error fetching unique objects from {table_name}: {e}")
            return []



def main():
    db = PostgresDB(host=DB_HOST, user=DB_USER, port=DB_PORT, password=DB_PASSWORD, db_name=DB_NAME)

    # Create 'objects' table
    unique_constraints = ['source_URL']
    db.create_table("objects", OBJECT_SCHEMA, unique_constraints=unique_constraints)
    
    # Create 'unique_records' table
    unique_constraints = ['source_URL']
    db.create_table("unique_records", UNIQUE_RECORD_SCHEMA, unique_constraints=unique_constraints)



    # Create 'users' table
    foreign_keys_users = [("property", "objects", "id")]
    unique_constraints = ['phone_number']
    db.create_table("users", USER_SCHEMA, foreign_keys=foreign_keys_users, unique_constraints=unique_constraints)

    objects_data = [
        {
            "id": 1,
            "category": "real_estate",
            "type": "apartment",
            "owner": "John Doe",
            "title": "Beautiful Apartment",
            "price": 500000,
            "price_for": "rent",
            "location": "New York, USA",
            "photo_URLs": ["https://example.com/photo1.jpg", "https://example.com/photo2.jpg"],
            "source_URL": "https://www.example.com/apartment",
            "last_updated": "2023-07-01 12:00:00"
        }
    ]
    user_data = [
        {
            "username": "johndoe",
            "phone_number": "1234567890",
            "email": "3Xg3O@example.com",
            "first_name": "John",
            "last_name": "Doe",
            "address": "123 Main St, Anytown, USA",
            "gender": "male",
            "property": 1,
            "last_updated": "2023-07-01 12:00:00"

        }
    ]
    db.save_to_db("objects", objects_data)
    db.save_to_db("unique_records", objects_data)
    db.save_to_db("users", user_data)




if __name__ == "__main__":
    main()
