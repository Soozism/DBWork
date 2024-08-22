import unittest
from sqliteDB import SQLiteORM

class TestSQLiteORM(unittest.TestCase):

    def setUp(self):
        """Set up a fresh database connection for each test."""
        self.db_name = "test.db"
        self.orm = SQLiteORM(self.db_name)
        # Create a fresh table for each test
        self.table_name = "test_table"
        columns = {"id": "INTEGER PRIMARY KEY", "name": "TEXT", "age": "INTEGER"}
        self.orm.create_table(self.table_name, columns)

    def tearDown(self):
        """Tear down the database connection and remove the test database."""
        if self.orm.table_exists(self.table_name):
            self.orm.drop_table(self.table_name)
        self.orm.close()


    def test_create_table(self):
        """Test if the table is created successfully."""
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table_name}';"
        self.orm.cursor.execute(query)
        result = self.orm.cursor.fetchone()
        self.assertIsNotNone(result, "Table should be created successfully")

    def test_insert_row(self):
        """Test inserting a valid row."""
        data = {"name": "Alice", "age": 30}
        row_id = self.orm.create(self.table_name, **data)
        self.assertIsNotNone(row_id, "Row ID should be returned after insert")

    def test_retrieve_single_row(self):
        """Test retrieving a single row with valid conditions."""
        data = {"name": "Alice", "age": 30}
        self.orm.create(self.table_name, **data)
        result = self.orm.get(self.table_name, name="Alice")
        self.assertEqual(result[1], "Alice", "Name should be Alice")
        self.assertEqual(result[2], 30, "Age should be 30")

    def test_retrieve_multiple_rows(self):
        """Test retrieving multiple rows that match certain conditions."""
        data1 = {"name": "Alice", "age": 30}
        data2 = {"name": "Bob", "age": 25}
        self.orm.create(self.table_name, **data1)
        self.orm.create(self.table_name, **data2)
        results = self.orm.filter(self.table_name, age=30)
        self.assertEqual(len(results), 1, "Should return one row with age 30")
        self.assertEqual(results[0][1], "Alice", "Name should be Alice")

    def test_update_row(self):
        """Test updating a row."""
        data = {"name": "Alice", "age": 30}
        self.orm.create(self.table_name, **data)
        updated_data = {"name": "Alice Updated"}
        rows_updated = self.orm.update(self.table_name, {"name": "Alice"}, **updated_data)
        self.assertEqual(rows_updated, 1, "One row should be updated")
        result = self.orm.get(self.table_name, name="Alice Updated")
        self.assertEqual(result[1], "Alice Updated", "Name should be updated to 'Alice Updated'")

    def test_delete_row(self):
        """Test deleting a row."""
        data = {"name": "Alice", "age": 30}
        self.orm.create(self.table_name, **data)
        rows_deleted = self.orm.delete(self.table_name, name="Alice")
        self.assertEqual(rows_deleted, 1, "One row should be deleted")
        result = self.orm.get(self.table_name, name="Alice")
        self.assertIsNone(result, "Row should no longer exist")

    def test_count_rows(self):
        """Test counting rows with and without conditions."""
        data1 = {"name": "Alice", "age": 30}
        data2 = {"name": "Bob", "age": 25}
        self.orm.create(self.table_name, **data1)
        self.orm.create(self.table_name, **data2)

        count_all = self.orm.count(self.table_name)
        count_filtered = self.orm.count(self.table_name, age=30)
        self.assertEqual(count_all, 2, "There should be two rows in total")
        self.assertEqual(count_filtered, 1, "There should be one row with age 30")

    def test_exists(self):
        """Test checking if a row exists."""
        data = {"name": "Alice", "age": 30}
        self.orm.create(self.table_name, **data)
        exists = self.orm.exists(self.table_name, name="Alice")
        self.assertTrue(exists, "Row with name 'Alice' should exist")

    def test_create_transaction(self):
        """Test transaction with create operation."""
        # Start a transaction and insert a row
        self.orm.begin_transaction()
        self.orm.create(self.table_name, name="Alice", age=30)
        self.orm.commit_transaction()

        # Verify that the row is inserted after commit
        result = self.orm.get(self.table_name, name="Alice")
        self.assertIsNotNone(result, "Row should exist after commit")

        # Insert another row and rollback
        self.orm.begin_transaction()
        self.orm.create(self.table_name, name="Bob", age=25)
        self.orm.rollback_transaction()

        # Verify that the row was not inserted after rollback
        result = self.orm.get(self.table_name, name="Bob")
        self.assertIsNone(result, "Row should not exist after rollback")

    def test_update_transaction(self):
        """Test transaction with update operation."""
        # Insert a row and commit
        self.orm.create(self.table_name, name="Alice", age=30)

        # Start a transaction and update the row
        self.orm.begin_transaction()
        self.orm.update(self.table_name, {"name": "Alice"}, name="Alice Updated")
        self.orm.commit_transaction()

        # Verify that the row is updated after commit
        result = self.orm.get(self.table_name, name="Alice Updated")
        self.assertIsNotNone(result, "Row should be updated after commit")
        self.assertEqual(result[1], "Alice Updated", "Name should be updated to 'Alice Updated'")

        # Start another transaction and update the row, but rollback
        self.orm.begin_transaction()
        self.orm.update(self.table_name, {"name": "Alice Updated"}, name="Alice Rolled Back")
        self.orm.rollback_transaction()

        # Verify that the update was not applied after rollback
        result = self.orm.get(self.table_name, name="Alice Rolled Back")
        self.assertIsNone(result, "Update should not be applied after rollback")
        result = self.orm.get(self.table_name, name="Alice Updated")
        self.assertIsNotNone(result, "Row should remain unchanged after rollback")

    def test_delete_transaction(self):
        """Test transaction with delete operation."""
        # Insert a row
        self.orm.create(self.table_name, name="Alice", age=30)

        # Start a transaction and delete the row
        self.orm.begin_transaction()
        self.orm.delete(self.table_name, name="Alice")
        self.orm.commit_transaction()

        # Verify that the row is deleted after commit
        result = self.orm.get(self.table_name, name="Alice")
        self.assertIsNone(result, "Row should be deleted after commit")

        # Insert another row for rollback test
        self.orm.create(self.table_name, name="Bob", age=25)

        # Start another transaction and delete the row, but rollback
        self.orm.begin_transaction()
        self.orm.delete(self.table_name, name="Bob")
        self.orm.rollback_transaction()

        # Verify that the row was not deleted after rollback
        result = self.orm.get(self.table_name, name="Bob")
        self.assertIsNotNone(result, "Row should not be deleted after rollback")

    def test_multiple_operations_transaction(self):
        """Test transaction with create, update, and delete operations."""
        # Start a transaction with multiple operations
        self.orm.begin_transaction()
        self.orm.create(self.table_name, name="Alice", age=30)
        self.orm.create(self.table_name, name="Bob", age=25)
        self.orm.update(self.table_name, {"name": "Alice"}, age=31)
        self.orm.delete(self.table_name, name="Bob")
        self.orm.commit_transaction()

        # Verify that the operations were applied after commit
        result = self.orm.get(self.table_name, name="Alice")
        self.assertIsNotNone(result, "Alice should exist after commit")
        self.assertEqual(result[2], 31, "Alice's age should be updated to 31")

        result = self.orm.get(self.table_name, name="Bob")
        self.assertIsNone(result, "Bob should be deleted after commit")

        # Start another transaction and rollback the operations
        self.orm.begin_transaction()
        self.orm.create(self.table_name, name="Charlie", age=35)
        self.orm.update(self.table_name, {"name": "Alice"}, age=32)
        self.orm.rollback_transaction()

        # Verify that the rollback reverted the changes
        result = self.orm.get(self.table_name, name="Charlie")
        self.assertIsNone(result, "Charlie should not exist after rollback")

        result = self.orm.get(self.table_name, name="Alice")
        self.assertIsNotNone(result, "Alice should remain after rollback")
        self.assertEqual(result[2], 31, "Alice's age should remain unchanged after rollback")

    def test_introspect_table(self):
        """Test introspecting the table's structure."""
        result = self.orm.introspect_table(self.table_name)
        self.assertEqual(len(result), 3, "Table should have three columns")
        self.assertEqual(result[0][1], "id", "First column should be 'id'")
        self.assertEqual(result[1][1], "name", "Second column should be 'name'")
        self.assertEqual(result[2][1], "age", "Third column should be 'age'")

    def test_invalid_insert(self):
        """Test inserting with missing required columns."""
        with self.assertRaises(Exception):
            self.orm.create(self.table_name, age="30")  # Missing 'name' column

    def test_close_database_connection(self):
        """Test closing the database connection."""
        self.orm.close()
        self.assertIsNone(self.orm.connection, "Connection should be closed")

    def test_table_exists(self):
        table_name = "test_table"
        columns = {"id": "INTEGER", "name": "TEXT"}

        # Create the table
        self.orm.create_table(table_name, columns)

        # Check if the table exists
        self.assertTrue(self.orm.table_exists(table_name))

    def test_drop_table(self):
        table_name = "test_table"
        columns = {"id": "INTEGER", "name": "TEXT"}
        self.orm.create_table(table_name, columns)

        # Drop the table
        self.orm.drop_table(table_name)

        # Check if the table has been dropped
        result = self.orm.introspect_table(table_name)
        self.assertEqual(result, [], f"The table '{table_name}' should have been dropped.")

if __name__ == '__main__':
    unittest.main()
