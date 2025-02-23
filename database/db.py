import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, execute_batch

from core.settings import DB_SCHEMA, DB_USER, DB_PASSWORD, DB_HOST


class PostgresDB:
    """ A class for Postgres database operations. """

    def __init__(self, host, user, port, password, db_name, db_schema):
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.db_name = db_name
        self.db_schema = db_schema
        if self.db_schema:
            self.valid_table_names = [table_schema['table_name'] for table_schema in self.db_schema.values()]
        self.conn = self.__connect()

    def __connect(self):
        """Establish a database connection."""
        conn = None
        cursor = None
        try:
            # Connect to the default "postgres" database
            conn = psycopg2.connect(dbname="postgres", user=self.user, password=self.password, host=self.host)
            conn.autocommit = True  # Enable autocommit to run the CREATE DATABASE statement
            cursor = conn.cursor()

            try:
                # Try to connect to the desired database
                conn = psycopg2.connect(dbname=self.db_name, user=self.user, password=self.password, host=self.host)
                print(f"Connected to the existing database '{self.db_name}'.")
                cursor.close()
                return conn
            except psycopg2.OperationalError:
                # If the database does not exist, catch the error and create the database
                print(f"Database '{self.db_name}' does not exist. Creating it...")
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.db_name)))
                print(f"Database '{self.db_name}' created successfully.")

                # Close the cursor and connection of the default database
                cursor.close()
                conn.close()

                # Reconnect to the newly created database
                new_conn =  psycopg2.connect(dbname=self.db_name, user=self.user, password=self.password, host=self.host)
                print(f"Connected to the newly created database '{self.db_name}'.")
                return new_conn

        except Exception as e:
            print(f"Error connecting to the default database: {e}")
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    def create_table(self, table_schema):
        """
        Creates a table based on the provided schema, with optional foreign keys and constraints.

        Args:
            table_schema (dict): A dictionary representing the table schema, with keys:
                - columns (dict): A dictionary mapping column names to column types.
                - foreign_keys (list): A list of foreign key constraints, each containing three elements:
                    column name, referenced table name, and referenced column name.
                - unique_constraints (list): A list of unique constraint columns.

        Returns:
            None

        """
        try:
            # Extract table name, columns, foreign keys, and unique constraints from the current table schema
            table_name = table_schema['table_name']
            columns = table_schema['columns']
            foreign_keys = table_schema.get('foreign_keys', [])
            unique_constraints = table_schema.get('unique_constraints', [])

            if self.__check_table_exists(table_name):
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
            with self.conn as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_query)
                    print(f"Table '{table_name}' created successfully (if not existed).")
        except psycopg2.Error as e:
            print(f"Error during table creation: {e}")
        except Exception as e:
            print(f"{type(e).__name__} occurred during table creation: {e}")



    def create_indexes(self, table_schema):
        """
        Creates indexes for the specified table.

        Args:
            table_schema (dict): A dictionary representing the table schema, with keys:
                - table_name (str): The name of the table.
                - indexes (list): A list of index definitions, where each index is a list of two or three elements:
                    - Column name
                    - Index name (optional)
                    - Index type (optional)

        Returns:
            None
        """

        try:
            table_name = table_schema['table_name']
            indexes = table_schema.get('indexes', [])
            if not indexes:
                print("No indexes provided.")
                return
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


    def __check_table_exists(self, table_name):
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

        try:
            # Validate table name
            if table_name not in self.valid_table_names:
                raise ValueError(f"Invalid table name: {table_name}")
            if not self.__check_table_exists(table_name):
                raise ValueError(f"Table '{table_name}' does not exist.")

            # Extract columns and values from the first row of data
            columns = data[0].keys()
            values = [[row[col] for col in columns] for row in data]

            insert_query = f"""
                                INSERT INTO {table_name} ({', '.join(columns)})
                                VALUES %s
                                ON CONFLICT (id) DO UPDATE SET
                                {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])};
                            """

            with self.conn as conn:
                with conn.cursor() as cursor:
                    execute_values(cursor, insert_query, values)
                    print(f"Inserted/Updated {len(data)} rows into '{table_name}'.")
        except psycopg2.Error as e:
            print(f"Database error during data insertion: {e}")
        except ValueError as e:
            print(f"Value error during data insertion: {e}")

        except Exception as e:
            print(f"{type(e).__name__} occurred during insertion: {e}")


    def read_from_db(self, table_name, columns=None):
        """Read data from the specified table. Optionally specify columns to fetch."""
        try:
            # Validate table name
            if table_name not in self.valid_table_names:
                raise ValueError(f"Invalid table name: {table_name}")
            if not self.__check_table_exists(table_name):
                raise ValueError(f"Table '{table_name}' does not exist.")

            # Determine which columns to select
            if columns is None:
                # If no columns are specified, select all columns
                query = f"SELECT * FROM {table_name};"
            else:
                # If specific columns are provided, use them in the SELECT statement
                columns_str = ", ".join(columns)
                query = f"SELECT {columns_str} FROM {table_name};"

            with self.conn as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    if not rows:
                        print(f"No records found in table '{table_name}'.")
                    return rows
        except psycopg2.Error as e:
            print(f"Error during data read: {e}")
        except ValueError as e:
            print(f"Error during data read: {e}")
        except Exception as e:
            print(f"{type(e).__name__} occurred during read: {e}")


class DailyParserDB(PostgresDB):

    def get_existing_object_ids(self, table_name, category_name):
        """Fetch all existing object IDs for a given category."""
        with self.conn as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT id FROM {table_name} WHERE category = %s;", (category_name,))
                return {row[0] for row in cursor.fetchall()}

    def remove_assigned_objects_from_unique_records(self, assigned_object_ids, table_name='unique_records'):
        """Batch removal of assigned objects from unique_records."""
        if not assigned_object_ids:
            return
        with self.conn as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    DELETE FROM {table_name}
                    WHERE id = ANY(%s);
                """, (list(assigned_object_ids),))
            print(f"Removed {len(assigned_object_ids)} objects from '{table_name}'.")


    def filter_out_unique_objects_by_category(self, category_name):
        """Fetch unique objects from the 'unique_records' table for a specific category."""

        try:
            table_name = self.db_schema.get('unique_records', {}).get('table_name', {})
            columns = self.db_schema.get('unique_records', {}).get('columns', {}).keys()
            column_names = ", ".join(columns)

            if not table_name or not columns:
                raise ValueError("Table name or columns not found in the database schema. Check your configuration.")

            with self.conn as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT {column_names}
                        FROM {table_name}
                        WHERE category = %s;
                    """, (category_name,))
                    rows = cursor.fetchall()

                    # Ensure that rows are properly fetched
                    if not rows:
                        print(f"No records found in table '{table_name}' for category '{category_name}'.")

                    # Map rows into a list of dictionaries
                    unique_objects = [dict(zip(columns, row)) for row in rows]
                    print(f"Fetched {len(unique_objects)} unique objects from the database.")
                    return unique_objects

        except ValueError as e:
            print(e)
        except psycopg2.DatabaseError as e:
            print(f"Error during data read: {e}")
        except Exception as e:
            print(f"Error fetching unique objects: {e}")
            return []


    def save_user_and_objects(self, user_data, assigned_objects):
        """Save user and assigned objects, and update the unique records in a single transaction."""
        if not user_data or not assigned_objects:
            print("No user data or assigned objects provided.")
            return
        try:
            with self.conn as conn:
                with conn.cursor() as cursor:
                    # Save user data into the 'users' table
                    print(f"Saving user data into 'users'...")
                    cursor.execute("""
                        INSERT INTO users (username, phone_number, email, first_name, last_name, address, gender)
                        VALUES (%s, %s, %s, %s, %s, %s, %s) 
                        ON CONFLICT(username, phone_number) DO NOTHING
                        RETURNING id;
                    """, (
                        user_data['username'],
                        user_data['phone_number'],
                        user_data['email'],
                        user_data['first_name'],
                        user_data['last_name'],
                        user_data['address'],
                        user_data['gender']
                    ))

                    user_id = cursor.fetchone()[0]

                    # Save the assigned objects to the 'objects' table
                    print(f"Saving {len(assigned_objects)} assigned objects into 'objects'...")
                    # Prepare the values list for bulk insertion or update
                    values = [
                        (
                            obj['id'],
                            obj['category'],
                            obj['type'], obj['title'],
                            obj['price'],
                            obj['price_for'],
                            obj['location'],
                            obj['photo_URLs'],
                            obj['source_URL'],
                            user_id
                        )
                        for obj in assigned_objects
                    ]
                    query = """
                        INSERT INTO objects (id, category, type, title, price, price_for, location, photo_URLs, source_URL, user_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            category = EXCLUDED.category,
                            type = EXCLUDED.type,
                            title = EXCLUDED.title,
                            price = EXCLUDED.price,
                            price_for = EXCLUDED.price_for,
                            location = EXCLUDED.location,
                            photo_URLs = EXCLUDED.photo_URLs,
                            source_URL = EXCLUDED.source_URL,
                            user_id = EXCLUDED.user_id;
                    """
                    cursor.executemany(query, values)

                    # Remove assigned objects from 'unique_records'
                    print(f"Removing {len(assigned_objects)} assigned objects from 'unique_records'.")
                    values = [(obj['id'],) for obj in assigned_objects]
                    cursor.executemany("""
                        DELETE FROM unique_records
                        WHERE id = %s;
                    """, values)

                    print(f"Successfully saved user {user_data['username']} and assigned {len(assigned_objects)} objects.")
                    return user_id

        except Exception as e:
            print(f"Error during transaction: {e}")

        return None



