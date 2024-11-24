import pandas as pd
import logging
import configparser
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to authenticate and create DB URI from .ini file
def auth(config_file, section):
    config = configparser.ConfigParser()
    config.read(config_file)

    print(f"Sections found in {config_file}: {config.sections()}")

    user = config.get(section, 'user')
    password = config.get(section, 'password')
    host = config.get(section, 'host')
    port = config.get(section, 'port')
    database = config.get(section, 'database')

    # Create connection URL
    connection_url = URL.create(
        drivername='postgresql',
        username=user,
        password=password,
        host=host,
        port=port,
        database=database
    )
    return connection_url

# Read the DB connection URI from the .ini file
CONFIG_FILE = '/Users/szjm/A9/config.ini'
DB_URI = auth(CONFIG_FILE, 'postgresql')  # Fetch the connection URL

# File paths (corrected relative paths)
BRANCH_SALES_PATH = '/Users/szjm/A9/data/Branch_Sales_Data_With_Issues.csv'
ONLINE_SALES_PATH = '/Users/szjm/A9/data/Online_Sales_Data_With_Issues.csv'
INVENTORY_PATH = '/Users/szjm/A9/data/Inventory_Data_With_Issues.csv'
CUSTOMERS_PATH = '/Users/szjm/A9/data/Customer_Data_With_Issues.csv'

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
