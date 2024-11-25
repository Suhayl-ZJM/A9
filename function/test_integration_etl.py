import unittest
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from etl_pipeline import auth, extract_data, transform_data, load_data_to_db

class TestETLIntegration(unittest.TestCase):
    """Integration tests for the ETL pipeline."""

    @classmethod
    def setUpClass(cls):
        """Set up the database connection and create a test engine."""
        CONFIG_FILE = '/Users/szjm/A9/config.ini'
        cls.engine = create_engine(auth(CONFIG_FILE, 'postgresql'))

    def setUp(self):
        """Prepare sample data for testing."""
        self.sample_branch_sales = pd.DataFrame({
            'transaction_id': [1, 2],
            'branch_id': [101, 102],
            'timestamp': ['11/24/2024', 'Invalid Date'],
            'item_id': [201, 202],
            'quantity': [5, None],
            'price': [20.0, None]
        })

        self.sample_online_sales = pd.DataFrame({
            'transaction_id': [1],
            'branch_id': [101],
            'timestamp': ['11/24/2024'],
            'item_id': [201],
            'quantity': [10],
            'price': [50.0],
            'delivery_address': [None]
        })

        self.sample_customer_data = pd.DataFrame({
            'customer_id': [1, 2],
            'email': ['TEST@EMAIL.COM', None],
            'loyalty_status': ['Gold', None]
        })

        self.sample_inventory_data = pd.DataFrame({
            'item_id': [201, 202],
            'branch_id': [101, 102],
            'stock_level': [50, None],
            'reorder_level': [20, 30]
        })

    def test_etl_pipeline_integration(self):
        """Test the ETL pipeline end-to-end integration with the database."""

        # Step 1: Transform data
        branch_sales_transformed, online_sales_transformed, customer_data_transformed, inventory_data_transformed = transform_data(
            self.sample_branch_sales,
            self.sample_online_sales,
            self.sample_customer_data,
            self.sample_inventory_data
        )

        # Step 2: Load transformed data into the database
        load_data_to_db(branch_sales_transformed, 'test_branch_sales', self.engine)
        load_data_to_db(online_sales_transformed, 'test_online_sales', self.engine)
        load_data_to_db(customer_data_transformed, 'test_customer_data', self.engine)
        load_data_to_db(inventory_data_transformed, 'test_inventory_data', self.engine)

        # Step 3: Verify data in the database
        with self.engine.connect() as conn:
            # Use SQLAlchemy inspector to check if the tables exist
            inspector = inspect(conn)
            tables = inspector.get_table_names()
            self.assertIn('test_branch_sales', tables)
            self.assertIn('test_online_sales', tables)
            self.assertIn('test_customer_data', tables)
            self.assertIn('test_inventory_data', tables)

            # Verify row counts for each table
            for table_name, expected_rows in [
                ('test_branch_sales', len(branch_sales_transformed)),
                ('test_online_sales', len(online_sales_transformed)),
                ('test_customer_data', len(customer_data_transformed)),
                ('test_inventory_data', len(inventory_data_transformed)),
            ]:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.scalar()
                self.assertEqual(row_count, expected_rows)

            # Verify column names for branch_sales
            branch_sales_columns = [col['name'] for col in inspector.get_columns('test_branch_sales')]
            self.assertListEqual(
                branch_sales_columns,
                ['transaction_id', 'branch_id', 'timestamp', 'item_id', 'quantity', 'price', 'total_sale']
            )

    def tearDown(self):
        """Clean up the database after each test."""
        with self.engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS test_branch_sales"))
            conn.execute(text("DROP TABLE IF EXISTS test_online_sales"))
            conn.execute(text("DROP TABLE IF EXISTS test_customer_data"))
            conn.execute(text("DROP TABLE IF EXISTS test_inventory_data"))

    @classmethod
    def tearDownClass(cls):
        """Dispose of the test engine."""
        cls.engine.dispose()

if __name__ == '__main__':
    unittest.main()
