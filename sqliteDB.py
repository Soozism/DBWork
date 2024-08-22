import sqlite3

class SQLiteORM:
    def __init__(self, db_name):
        """Initialize the SQLite connection and cursor."""
        try:
            self.connection = sqlite3.connect(db_name, check_same_thread=False, isolation_level=None)
            self.cursor = self.connection.cursor()
            self.in_transaction = False  # Track if a transaction is active
        except sqlite3.Error as e:
            print(f"Error connecting to the database: {e}")
            raise Exception(f"Unable to connect to the database: {e}")

    def create_table(self, table_name, columns):
        """Create a new table if it doesn't exist."""
        try:
            column_definitions = ', '.join([f'{col} {dtype}' for col, dtype in columns.items()])
            query = f'CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions});'
            self.cursor.execute(query)
            self.connection.commit()
        except sqlite3.Error as e:
            print(f"Error creating table: {e}")
            raise Exception(f"Table creation failed: {e}")

    def get_table_schema(self, table_name):
        """Retrieve the schema of the table."""
        try:
            query = f'PRAGMA table_info({table_name});'
            self.cursor.execute(query)
            schema = self.cursor.fetchall()
            return {col[1]: col[2] for col in schema}  # col[1] is column name, col[2] is data type
        except sqlite3.Error as e:
            print(f"Error retrieving table schema: {e}")
            return {}

    def create(self, table_name, **kwargs):
        """Insert a new row into the table safely with data type checking."""
        if not kwargs:
            raise ValueError("No data provided for insertion")

        schema = self.get_table_schema(table_name)
        if not schema:
            raise ValueError(f"Table '{table_name}' does not exist or could not be retrieved")

        for col, value in kwargs.items():
            if col not in schema:
                raise ValueError(f"Column '{col}' does not exist in table '{table_name}'")
            expected_type = schema[col].upper()
            if expected_type == 'INTEGER' and not isinstance(value, int):
                raise TypeError(f"Value for column '{col}' should be of type INTEGER")
            elif expected_type == 'TEXT' and not isinstance(value, str):
                raise TypeError(f"Value for column '{col}' should be of type TEXT")
            elif expected_type == 'REAL' and not isinstance(value, (int, float)):
                raise TypeError(f"Value for column '{col}' should be of type REAL")
            elif expected_type == 'BLOB' and not isinstance(value, (bytes, bytearray)):
                raise TypeError(f"Value for column '{col}' should be of type BLOB")

        try:
            # Start a transaction if not already in one
            in_self_trasaction = False
            if not self.in_transaction:
                in_self_trasaction = True
                self.begin_transaction()

            columns = ', '.join(kwargs.keys())
            placeholders = ', '.join(['?' for _ in kwargs.values()])
            values = tuple(kwargs.values())
            query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders});'
            self.cursor.execute(query, values)

            # Commit the transaction if this method started it
            if not self.in_transaction or in_self_trasaction:
                self.commit_transaction()

            return self.cursor.lastrowid
        except sqlite3.Error as e:
            print(f"Error inserting row: {e}")
            if self.in_transaction:
                self.rollback_transaction()
            raise Exception(f"Insertion failed: {e}")

    def get(self, table_name, **kwargs):
        """Retrieve a single row that matches the query, safe from SQL injection."""
        if not kwargs:
            raise ValueError("No conditions provided for the query")
        try:
            conditions = ' AND '.join([f'{col}=?' for col in kwargs.keys()])
            values = tuple(kwargs.values())
            query = f'SELECT * FROM {table_name} WHERE {conditions} LIMIT 1;'
            self.cursor.execute(query, values)
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            print(f"Error fetching data: {e}")
            return None

    def filter(self, table_name, **kwargs):
        """Retrieve multiple rows that match the query safely."""
        try:
            if not kwargs:
                query = f'SELECT * FROM {table_name};'
                self.cursor.execute(query)
            else:
                conditions = ' AND '.join([f'{col}=?' for col in kwargs.keys()])
                values = tuple(kwargs.values())
                query = f'SELECT * FROM {table_name} WHERE {conditions};'
                self.cursor.execute(query, values)
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Error filtering data: {e}")
            return []

    def update(self, table_name, filters, **kwargs):
        """Update rows that match the filter securely."""
        if not filters or not kwargs:
            raise ValueError("Both filters and update data must be provided")
        try:

            in_self_trasaction = False
            if not self.in_transaction:
                in_self_trasaction = True
                self.begin_transaction()

            set_clause = ', '.join([f'{col}=?' for col in kwargs.keys()])
            filter_clause = ' AND '.join([f'{col}=?' for col in filters.keys()])
            values = tuple(kwargs.values()) + tuple(filters.values())
            query = f'UPDATE {table_name} SET {set_clause} WHERE {filter_clause};'
            self.cursor.execute(query, values)

            # Commit the transaction if this method started it
            if not self.in_transaction or in_self_trasaction:
                self.commit_transaction()

            return self.cursor.rowcount
        except sqlite3.Error as e:
            print(f"Error updating row: {e}")
            self.connection.rollback()
            raise Exception(f"Update failed: {e}")

    def delete(self, table_name, **kwargs):
        """Delete rows without using placeholders."""
        try:
            in_self_trasaction = False
            if not self.in_transaction:
                in_self_trasaction = True
                self.begin_transaction()

            conditions = ' AND '.join([f"{col}='{val}'" for col, val in kwargs.items()])
            print(conditions)
            query = f"DELETE FROM {table_name} WHERE {conditions};"
            print(query)
            self.cursor.execute(query)

            # Commit the transaction if this method started it
            if not self.in_transaction or in_self_trasaction:
                self.commit_transaction()

            return self.cursor.rowcount
        except sqlite3.Error as e:
            print(f"Error deleting row: {e}")
            self.connection.rollback()
            raise Exception(f"Deletion failed: {e}")

    def all(self, table_name):
        """Retrieve all rows from the table safely."""
        try:
            query = f'SELECT * FROM {table_name};'
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Error fetching all rows: {e}")
            return []

    def exists(self, table_name, **kwargs):
        """Check if a row exists based on conditions safely."""
        if not kwargs:
            raise ValueError("Conditions must be provided to check existence")
        try:
            conditions = ' AND '.join([f'{col}=?' for col in kwargs.keys()])
            values = tuple(kwargs.values())
            query = f'SELECT 1 FROM {table_name} WHERE {conditions} LIMIT 1;'
            self.cursor.execute(query, values)
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            print(f"Error checking existence: {e}")
            return False

    def count(self, table_name, **kwargs):
        """Count the number of rows that match the conditions."""
        try:
            if kwargs:
                conditions = ' AND '.join([f'{col}=?' for col in kwargs.keys()])
                values = tuple(kwargs.values())
                query = f'SELECT COUNT(*) FROM {table_name} WHERE {conditions};'
                self.cursor.execute(query, values)
            else:
                query = f'SELECT COUNT(*) FROM {table_name};'
                self.cursor.execute(query)
            return self.cursor.fetchone()[0]
        except sqlite3.Error as e:
            print(f"Error counting rows: {e}")
            return 0

    def begin_transaction(self):
        """Start a transaction."""
        try:
            self.connection.execute('BEGIN TRANSACTION;')
            self.in_transaction = True
            print("Transaction started.")
        except sqlite3.Error as e:
            print(f"Error starting transaction: {e}")
            raise Exception(f"Transaction start failed: {e}")

    def commit_transaction(self):
        """Commit the current transaction safely."""
        try:
            self.connection.commit()
            self.in_transaction = False  # Reset the flag
        except sqlite3.Error as e:
            print(f"Error committing transaction: {e}")
            self.connection.rollback()  # Rollback if commit fails
            self.in_transaction = False  # Reset the flag
            raise Exception(f"Transaction commit failed: {e}")

    def rollback_transaction(self):
        """Rollback the current transaction safely."""
        if self.in_transaction:
            try:
                self.connection.rollback()
                self.in_transaction = False  # Reset the flag
                print("Transaction rolled back.")
            except sqlite3.Error as e:
                print(f"Error rolling back transaction: {e}")
                raise Exception(f"Transaction rollback failed: {e}")
        else:
            print("No active transaction to rollback.")

    def close(self):
        """Close the connection to the database safely."""
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
        except sqlite3.Error as e:
            print(f"Error closing connection: {e}")
            raise Exception(f"Connection close failed: {e}")

    def introspect_table(self, table_name):
        """Introspect a table's structure (columns and their types) safely."""
        try:
            query = f'PRAGMA table_info({table_name});'
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Error introspecting table: {e}")
            return []

    def table_exists(self, table_name):
        """Check if a table exists in the database."""
        try:
            query = f"SELECT name FROM sqlite_master WHERE type='table' AND name=?;"
            self.cursor.execute(query, (table_name,))
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            print(f"Error checking if table exists: {e}")
            return False

    def drop_table(self, table_name):
        """Drop a table from the database."""
        try:
            query = f"DROP TABLE IF EXISTS {table_name};"
            self.cursor.execute(query)
            self.connection.commit()
            print(f"Table '{table_name}' has been dropped successfully.")
        except sqlite3.Error as e:
            print(f"Error dropping table: {e}")
            self.connection.rollback()
            raise Exception(f"Table drop failed: {e}")