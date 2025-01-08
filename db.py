import psycopg2
from psycopg2.extras import execute_values


DB_HOST = 'localhost'
DB_PORT='5432'
DB_USER = 'aldmikon'
DB_PASSWORD = '123'
DB_NAME = 'avito'
DB_TABLE_NAME = 'real_estate'
PRODUCT_SCHEMA = {
    "id": "BIGINT PRIMARY KEY",
    "category": "TEXT",
    "title": "TEXT",
    "price": "TEXT",
    "price_for": "TEXT",
    "location": "TEXT",
    "photos": "TEXT[]",
    "URL": "TEXT",
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

    def __connect(self):
        """Establish a database connection."""
        return psycopg2.connect(
            database=self.db_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    def create_table(self, table_name, schema):
        """
        Creates a table based on the provided schema.
        :param table_name: Name of the table.
        :param schema: Dictionary defining column names and types.
        """
        columns = ", ".join([f"{col_name} {col_type}" for col_name, col_type in schema.items()])
        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"

        try:
            with self.__connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_query)
                    print(f"Table '{table_name}' created successfully.")

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



def save_to_postgres(data: list[dict]) -> None:
    db = PostgresDB(DB_HOST, DB_USER, DB_PORT, DB_PASSWORD, DB_NAME)
    db.create_table(DB_TABLE_NAME,PRODUCT_SCHEMA)
    db.save_to_db(DB_TABLE_NAME, data)
