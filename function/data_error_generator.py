import pandas as pd

# File paths
BRANCH_SALES_PATH = '/Users/szjm/A9/data/Branch_Sales_Data.csv'
ONLINE_SALES_PATH = '/Users/szjm/A9/data/Online_Sales_Data.csv'
INVENTORY_PATH = '/Users/szjm/A9/data/Inventory_Data.csv'
CUSTOMERS_PATH = '/Users/szjm/A9/data/Customer_Data.csv'

# Load CSVs
branch_sales = pd.read_csv(BRANCH_SALES_PATH)
customer_data = pd.read_csv(CUSTOMERS_PATH)
inventory_data = pd.read_csv(INVENTORY_PATH)
online_sales = pd.read_csv(ONLINE_SALES_PATH)

# Branch Sales: Add new rows with issues
new_branch_sales_rows = pd.DataFrame({
    'transaction_id': [999999, 999998],
    'branch_id': [1, None],  # Missing branch_id in one row
    'timestamp': ['2024-11-01 12:00:00', '2024-11-02 12:00:00'],
    'item_id': [101, 102],
    'quantity': [5, None],  # Missing quantity in one row
    'price': [None, 50.0],  # Missing price in one row
    'total_sale': [None, None]  # Will be calculated later
})
branch_sales_with_issues = pd.concat([branch_sales, new_branch_sales_rows], ignore_index=True)

# Customer Data: Add new rows with issues
new_customer_data_rows = pd.DataFrame({
    'customer_id': [888888, 888887],
    'name': ['New Customer 1', 'New Customer 2'],
    'email': [None, 'test@example.com'],  # Missing email in one row
    'loyalty_status': ['Gold', None]  # Missing loyalty_status in one row
})
customer_data_with_issues = pd.concat([customer_data, new_customer_data_rows], ignore_index=True)

# Inventory Data: Add new rows with issues
new_inventory_data_rows = pd.DataFrame({
    'item_id': [55555, 55556],
    'branch_id': [3, None],  # Missing branch_id in one row
    'stock_level': [100, 200],
    'reorder_level': [None, 10]  # Missing reorder_level in one row
})
inventory_data_with_issues = pd.concat([inventory_data, new_inventory_data_rows], ignore_index=True)

# Online Sales: Add new rows with issues
new_online_sales_rows = pd.DataFrame({
    'transaction_id': [777777, 777776],
    'customer_id': [888888, 888887],
    'timestamp': ['2024-11-01 14:00:00', '2024-11-02 15:00:00'],
    'item_id': [201, 202],
    'quantity': [2, None],  # Missing quantity in one row
    'price': [30.0, None],  # Missing price in one row
    'delivery_address': [None, '123 Main Street']  # Missing delivery address in one row
})
online_sales_with_issues = pd.concat([online_sales, new_online_sales_rows], ignore_index=True)

# Save new CSVs with issues
branch_sales_with_issues.to_csv('/Users/szjm/A9/data/Branch_Sales_Data_With_Issues.csv', index=False)
customer_data_with_issues.to_csv('/Users/szjm/A9/data/Customer_Data_With_Issues.csv', index=False)
inventory_data_with_issues.to_csv('/Users/szjm/A9/data/Inventory_Data_With_Issues.csv', index=False)
online_sales_with_issues.to_csv('/Users/szjm/A9/data/Online_Sales_Data_With_Issues.csv', index=False)
