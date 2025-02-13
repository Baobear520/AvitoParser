import psycopg2
from psycopg2.extras import execute_values


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

    def create_table(self, schema):
        """
        Creates a table based on the provided schema, with optional foreign keys and constraints.

        Args:
            schema (dict): A dictionary representing the table schema, with keys:
                - columns (dict): A dictionary mapping column names to column types.
                - foreign_keys (list): A list of foreign key constraints, each containing three elements:
                    column name, referenced table name, and referenced column name.
                - unique_constraints (list): A list of unique constraint columns.


        Returns:
            None

        """
        table_name = schema['table_name']
        columns = schema['columns']
        foreign_keys = schema.get('foreign_keys', [])
        unique_constraints = schema.get('constraints', [])

        if self.check_table_exists(table_name):
            print(f"Table '{table_name}' already exists. Skipping...")
            return

        # Define columns and their types
        columns = ", ".join([f"{col_name} {col_type}" for col_name, col_type in columns.items()])

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

        # # Define other constraints (if any)
        # if other_constraints:
        #     other_constraints_clause = f", {', '.join(other_constraints)}"
        # else:
        #     other_constraints_clause = ""

        # Combine all clauses
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
            {foreign_keys_clause}
            {unique_constraints_clause}
        );
        """

        # Execute the query to create the table
        try:
            with self.conn as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_query)
                    print(f"Table '{table_name}' created successfully (if not existed).")
        except psycopg2.Error as e:
            print(f"Error during table creation: {e}")
        except Exception as e:
            print(f"{type(e).__name__} occurred during table creation: {e}")

    def create_indexes(self, table_name, indexes):
        """
        Creates indexes for the specified table.

        :param table_name: Name of the table.
        :param indexes: List of column names or (column_name, index_name) for indexing.
        """
        if not indexes:
            print("No indexes provided.")
            return

        try:
            with self.conn as conn:
                with conn.cursor() as cursor:
                    for index in indexes:
                        # If index name is not provided, generate a default name
                        if len(index) == 1:
                            index_name = f"idx_{index[0]}"
                        else:
                            index_name = index[1]

                        index_query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({index[0]});"
                        cursor.execute(index_query)
                        print(f"Index '{index_name}' created for column '{index[0]}' in table '{table_name}'.")
        except psycopg2.Error as e:
            print(f"Error during index creation: {e}")
        except Exception as e:
            print(f"{type(e).__name__} occurred during index creation: {e}")

    def check_table_exists(self, table_name):
        """
        Checks if a table exists in the database.
        :param table_name: Name of the table.
        :return: True if the table exists, False otherwise.
        """
        try:
            with self.conn as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = '{table_name}');")
                    return cursor.fetchone()[0]
        except psycopg2.Error as e:
            print(f"Error during table existence check: {e}")
        except Exception as e:
            print(f"{type(e).__name__} occurred during table existence check: {e}")
        return False



    def save_to_db(self, table_name, data):
        """
        Inserts or updates data in the specified table.
        :param table_name: Name of the table.
        :param data: List of dictionaries representing rows to be inserted/updated.
        """
        if not data:
            print("No data to save.")
            return

        # Validate table name
        if not self.check_table_exists(table_name):
            print(f"Table '{table_name}' does not exist.")
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
            with self.conn as conn:
                with conn.cursor() as cursor:
                    execute_values(cursor, insert_query, values)
                    print(f"Inserted/Updated {len(data)} rows into '{table_name}'.")
        except psycopg2.Error as e:
            print(f"Error during data insertion: {e}")
        except Exception as e:
            print(f"{type(e).__name__} occurred during insertion: {e}")

    def read_from_db(self, table_name, columns=None):
        """Read data from the specified table. Optionally specify columns to fetch."""
        if not self.check_table_exists(table_name):
            print(f"Table '{table_name}' does not exist.")
            return

        # Determine which columns to select
        if columns is None:
            # If no columns are specified, select all columns
            query = f"SELECT * FROM {table_name};"
        else:
            # If specific columns are provided, use them in the SELECT statement
            columns_str = ", ".join(columns)
            query = f"SELECT {columns_str} FROM {table_name};"

        try:
            with self.conn as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    if not rows:
                        print(f"No records found in table '{table_name}'.")
                    return rows
        except psycopg2.Error as e:
            print(f"Error during data read: {e}")
        except Exception as e:
            print(f"{type(e).__name__} occurred during read: {e}")


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

