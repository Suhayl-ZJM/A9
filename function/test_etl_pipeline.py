import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
import logging
from etl_pipeline import auth, extract_data, transform_data, load_data_to_db

# Configure a separate logger for tests
test_logger = logging.getLogger("etl_test_logger")
test_logger.setLevel(logging.INFO)

# File handler for test logs
test_log_file = "/Users/szjm/A9/logs/etl_tests.log"
file_handler = logging.FileHandler(test_log_file)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

if test_logger.hasHandlers():
    test_logger.handlers.clear()

test_logger.addHandler(file_handler)

class TestETLPipeline(unittest.TestCase):

    def setUp(self):
        # Sample data for testing
        self.branch_sales_data = pd.DataFrame({
            'timestamp': ['01/01/2022', '02/15/2022', 'invalid_date'],
            'quantity': [10, 5, None],
            'price': [20.0, 15.5, None]
        })
        self.online_sales_data = pd.DataFrame({
            'timestamp': ['03/01/2022', 'invalid_date'],
            'quantity': [2, None],
            'price': [50.0, None],
            'delivery_address': [None, '123 Main St']
        })
        self.customer_data = pd.DataFrame({
            'email': ['USER@EXAMPLE.COM', 'test@example.com'],
            'loyalty_status': [None, 'Gold']
        })
        self.inventory_data = pd.DataFrame({
            'stock_level': [10, None],
            'reorder_level': [5, 8]
        })

    @patch('etl_pipeline.configparser.ConfigParser')
    def test_auth(self, mock_config_parser):
        mock_config_parser.return_value.get.side_effect = lambda section, key: {
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': '5432',
            'database': 'test_db'
        }.get(key)

        url = auth('dummy_config.ini', 'postgresql')
        self.assertTrue(str(url).startswith('postgresql+psycopg2://'))
        test_logger.info("Test 'test_auth' passed.")

    @patch('etl_pipeline.pd.read_csv')
    def test_extract_data(self, mock_read_csv):
        mock_read_csv.return_value = self.branch_sales_data
        df = extract_data('dummy_path.csv')
        pd.testing.assert_frame_equal(df, self.branch_sales_data)
        test_logger.info("Test 'test_extract_data' passed.")

    def test_transform_data(self):
        transformed = transform_data(
            self.branch_sales_data,
            self.online_sales_data,
            self.customer_data,
            self.inventory_data
        )

        # Check Branch Sales transformation
        branch_sales_transformed = transformed[0]
        self.assertEqual(branch_sales_transformed['quantity'].iloc[2], 0)
        self.assertEqual(branch_sales_transformed['total_sale'].iloc[0], 200)

        # Check Online Sales transformation
        online_sales_transformed = transformed[1]
        self.assertEqual(online_sales_transformed['delivery_address'].iloc[0], 'Unknown')

        # Check Customer Data transformation
        customer_transformed = transformed[2]
        self.assertEqual(customer_transformed['email'].iloc[0], 'user@example.com')

        # Check Inventory Data transformation
        inventory_transformed = transformed[3]
        self.assertTrue(inventory_transformed['reorder_status'].iloc[1]) 
        test_logger.info("Test 'test_transform_data' passed.")

    @patch('etl_pipeline.pd.DataFrame.to_sql')
    def test_load_data_to_db(self, mock_to_sql):
        engine = MagicMock()
        load_data_to_db(self.branch_sales_data, 'branch_sales', engine)
        mock_to_sql.assert_called_once_with('branch_sales', engine, if_exists='replace', index=False)
        test_logger.info("Test 'test_load_data_to_db' passed.")


if __name__ == '__main__':
    unittest.main()
