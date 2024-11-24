import pandas as pd
import logging
import configparser
from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read database configuration from properties file
config = configparser.ConfigParser()
config.read('../db_config.properties')  # Corrected path
DB_URI = config['DEFAULT']['DB_URI']  # Fetch the database URI

# File paths (corrected relative paths)
BRANCH_SALES_PATH = '../data/Branch_Sales_Data.csv'
ONLINE_SALES_PATH = '../data/Online_Sales_Data.csv'
INVENTORY_PATH = '../data/Inventory_Data.csv'
CUSTOMERS_PATH = '../data/Customer_Data.csv'

# ETL Pipeline Functions
def extract_data(file_path):
    """Extract data from a CSV file."""
    try:
        logging.info(f"Extracting data from {file_path}")
        return pd.read_csv(file_path)
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        raise

def transform_data(branch_sales, online_sales):
    """Transform data: standardise formats, handle missing values, and calculate metrics."""
    logging.info("Transforming branch sales data...")
    branch_sales['timestamp'] = pd.to_datetime(branch_sales['timestamp'])
    branch_sales.drop_duplicates(inplace=True)
    branch_sales['total_sale'] = branch_sales['quantity'] * branch_sales['price']

    logging.info("Transforming online sales data...")
    online_sales['timestamp'] = pd.to_datetime(online_sales['timestamp'])
    online_sales.drop_duplicates(inplace=True)
    online_sales['total_sale'] = online_sales['quantity'] * online_sales['price']

    return branch_sales, online_sales

def load_data_to_db(data, table_name, db_uri):
    """Load data into a PostgreSQL database."""
    try:
        logging.info(f"Loading data into {table_name}")
        engine = create_engine(db_uri)
        data.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Successfully loaded data into {table_name}")
    except Exception as e:
        logging.error(f"Error loading data into database: {e}")
        raise

# Main ETL Process
if __name__ == "__main__":
    # Extract
    branch_sales = extract_data(BRANCH_SALES_PATH)
    online_sales = extract_data(ONLINE_SALES_PATH)

    # Transform
    branch_sales_transformed, online_sales_transformed = transform_data(branch_sales, online_sales)

    # Load
    load_data_to_db(branch_sales_transformed, 'branch_sales', DB_URI)
    load_data_to_db(online_sales_transformed, 'online_sales', DB_URI)
