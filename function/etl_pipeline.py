import pandas as pd
import logging
import configparser
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/Users/szjm/A9/logs/etl_pipeline.log"),  # Log to file
        logging.StreamHandler()                          # Log to console
    ]
)

def auth(config_file, section):
    """Read database configuration and return DB URI."""
    config = configparser.ConfigParser()
    config.read(config_file)

    user = config.get(section, 'user')
    password = config.get(section, 'password')
    host = config.get(section, 'host')
    port = config.get(section, 'port')
    database = config.get(section, 'database')

    # Return connection URL as a string
    connection_url = URL.create(
        drivername='postgresql+psycopg2',
        username=user,
        password=password,
        host=host,
        port=port,
        database=database
    )
    return connection_url


def extract_data(file_path):
    """Extract data from a CSV file."""
    try:
        logging.info(f"Extracting data from {file_path}")
        return pd.read_csv(file_path)
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        raise


def transform_data(branch_sales, online_sales, customer_data, inventory_data):
    """Transform data: standardise formats, handle missing values, and calculate metrics."""
    try:
        # Transform Branch Sales Data
        logging.info("Transforming branch sales data...")
        branch_sales['timestamp'] = pd.to_datetime(branch_sales['timestamp'], format='%m/%d/%Y', errors='coerce')

        # Log rows with invalid timestamps
        invalid_timestamps = branch_sales[branch_sales['timestamp'].isna()]
        if not invalid_timestamps.empty:
            logging.warning(f"Found {len(invalid_timestamps)} rows with invalid timestamps in branch sales.")
            print(invalid_timestamps)

        # Fill invalid timestamps with a default value
        branch_sales['timestamp'].fillna(pd.Timestamp('1970-01-01 00:00:00'), inplace=True)
        branch_sales['quantity'].fillna(0, inplace=True)
        branch_sales['price'].fillna(0.0, inplace=True)
        branch_sales['total_sale'] = branch_sales['quantity'] * branch_sales['price']
        branch_sales.drop_duplicates(inplace=True)

        # Transform Online Sales Data
        logging.info("Transforming online sales data...")
        online_sales['timestamp'] = pd.to_datetime(online_sales['timestamp'], format='%m/%d/%Y', errors='coerce')

        # Log rows with invalid timestamps
        invalid_timestamps = online_sales[online_sales['timestamp'].isna()]
        if not invalid_timestamps.empty:
            logging.warning(f"Found {len(invalid_timestamps)} rows with invalid timestamps in online sales.")
            print(invalid_timestamps)

        # Fill invalid timestamps with a default value
        online_sales['timestamp'].fillna(pd.Timestamp('1970-01-01 00:00:00'), inplace=True)
        online_sales['quantity'].fillna(0, inplace=True)
        online_sales['price'].fillna(0.0, inplace=True)
        online_sales['delivery_address'].fillna('Unknown', inplace=True)
        online_sales['total_sale'] = online_sales['quantity'] * online_sales['price']
        online_sales.drop_duplicates(inplace=True)

        # Transform Customer Data
        logging.info("Transforming customer data...")
        customer_data = customer_data.drop_duplicates().assign(
            email=customer_data['email'].str.lower(),
            loyalty_status=customer_data['loyalty_status'].fillna('Unknown')
        )

        # Transform Inventory Data
        logging.info("Transforming inventory data...")
        inventory_data = inventory_data.drop_duplicates().assign(
            stock_level=inventory_data['stock_level'].fillna(0),
            reorder_level=inventory_data['reorder_level'].fillna(0),
            reorder_status=inventory_data['stock_level'] < inventory_data['reorder_level']
        )

        logging.info("Transformation complete.")
        return branch_sales, online_sales, customer_data, inventory_data

    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise


def load_data_to_db(data, table_name, engine):
    """Load data into a PostgreSQL database."""
    try:
        logging.info(f"Loading data into {table_name}")
        data.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Successfully loaded data into {table_name}")
    except Exception as e:
        logging.error(f"Error loading data into database: {e}")
        raise


if __name__ == "__main__":
    # Database configuration
    CONFIG_FILE = '/Users/szjm/A9/config.ini'
    DB_URI = auth(CONFIG_FILE, 'postgresql')  # Get the DB URI

    # Create a single engine instance
    engine = create_engine(DB_URI)

    # File paths
    BRANCH_SALES_PATH = '/Users/szjm/A9/data/Branch_Sales_Data_With_Issues.csv'
    ONLINE_SALES_PATH = '/Users/szjm/A9/data/Online_Sales_Data_With_Issues.csv'
    CUSTOMER_DATA_PATH = '/Users/szjm/A9/data/Customer_Data_With_Issues.csv'
    INVENTORY_DATA_PATH = '/Users/szjm/A9/data/Inventory_Data_With_Issues.csv'

    # ETL Pipeline
    branch_sales = extract_data(BRANCH_SALES_PATH)
    online_sales = extract_data(ONLINE_SALES_PATH)
    customer_data = extract_data(CUSTOMER_DATA_PATH)
    inventory_data = extract_data(INVENTORY_DATA_PATH)

    branch_sales_transformed, online_sales_transformed, customer_data_transformed, inventory_data_transformed = transform_data(
        branch_sales, online_sales, customer_data, inventory_data
    )

    # Load data into the database
    load_data_to_db(branch_sales_transformed, 'branch_sales', engine)
    load_data_to_db(online_sales_transformed, 'online_sales', engine)
    load_data_to_db(customer_data_transformed, 'customer_data', engine)
    load_data_to_db(inventory_data_transformed, 'inventory_data', engine)
