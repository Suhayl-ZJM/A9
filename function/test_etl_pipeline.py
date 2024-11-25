import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
from etl_pipeline import auth, extract_data, transform_data, load_data_to_db


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
        # Mock configuration file
        mock_config_parser.return_value.get.side_effect = lambda section, key: {
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': '5432',
            'database': 'test_db'
        }.get(key)

        url = auth('dummy_config.ini', 'postgresql')
        self.assertTrue(str(url).startswith('postgresql+psycopg2://'))

    @patch('etl_pipeline.pd.read_csv')
    def test_extract_data(self, mock_read_csv):
        mock_read_csv.return_value = self.branch_sales_data
        df = extract_data('dummy_path.csv')
        pd.testing.assert_frame_equal(df, self.branch_sales_data)

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
        self.assertFalse(inventory_transformed['reorder_status'].iloc[1])

    @patch('etl_pipeline.pd.DataFrame.to_sql')
    def test_load_data_to_db(self, mock_to_sql):
        # Mock the to_sql method to avoid actual database writes
        engine = MagicMock()
        load_data_to_db(self.branch_sales_data, 'branch_sales', engine)
        mock_to_sql.assert_called_once_with('branch_sales', engine, if_exists='replace', index=False)


if __name__ == '__main__':
    unittest.main()
