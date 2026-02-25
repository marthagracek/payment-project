import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Set random seed for reproducibility
np.random.seed(42)

# Define file paths
data_path = r'C:\Users\mknee\OneDrive\Desktop\payment-project\data\\'
files = {
    'orders': 'olist_orders_dataset.csv',
    'payments': 'olist_order_payments_dataset.csv',
    'customers': 'olist_customers_dataset.csv',
    'items': 'olist_order_items_dataset.csv'
}

print('Loading CSV files...')
# Read CSV files
orders = pd.read_csv(data_path + files['orders'])
payments = pd.read_csv(data_path + files['payments'])
customers = pd.read_csv(data_path + files['customers'])
items = pd.read_csv(data_path + files['items'])

print(f'Loaded {len(orders)} orders, {len(payments)} payments, {len(customers)} customers, {len(items)} items')

# Clean the data
print('\nCleaning data...')
orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
orders['order_approved_at'] = pd.to_datetime(orders['order_approved_at'])

# Create payment_failed column
orders['payment_failed'] = orders['order_status'].isin(['canceled', 'unavailable']).astype(int)

# Merge orders with payments
print('\nMerging orders with payments...')
enriched = orders.merge(payments, on='order_id', how='left')

# Simulate retry logic
print('\nSimulating retry logic...')
failed_mask = enriched['payment_failed'] == 1
num_failed = failed_mask.sum()

# Initialize retry columns
enriched['retry_attempted'] = 0
enriched['retry_success'] = 0

# For failed orders, 40% get retry attempt
retry_mask = failed_mask & (np.random.rand(len(enriched)) < 0.4)
enriched.loc[retry_mask, 'retry_attempted'] = 1

# For retried orders, 50% succeed
success_mask = retry_mask & (np.random.rand(len(enriched)) < 0.5)
enriched.loc[success_mask, 'retry_success'] = 1

# Add revenue_at_risk column
enriched['revenue_at_risk'] = enriched.apply(
    lambda row: row['payment_value'] if row['payment_failed'] == 1 else 0, 
    axis=1
)

# Create database connection
print('\nConnecting to PostgreSQL...')
connection_string = 'postgresql://postgres:postgres123@localhost:5432/payment_analysis'
engine = create_engine(connection_string)

# Load data into PostgreSQL
print('\nLoading data into PostgreSQL...')
orders.to_sql('raw_orders', engine, if_exists='replace', index=False)
print('Loaded raw_orders table')

payments.to_sql('raw_payments', engine, if_exists='replace', index=False)
print('Loaded raw_payments table')

customers.to_sql('raw_customers', engine, if_exists='replace', index=False)
print('Loaded raw_customers table')

items.to_sql('raw_items', engine, if_exists='replace', index=False)
print('Loaded raw_items table')

enriched.to_sql('enriched_orders', engine, if_exists='replace', index=False)
print('Loaded enriched_orders table')

# Print summary
print('\n' + '='*60)
print('SUMMARY')
print('='*60)
print(f'Total orders loaded: {len(orders):,}')
print(f'Total failed orders: {orders["payment_failed"].sum():,}')
print(f'Failed orders with retry attempted: {enriched[enriched["payment_failed"]==1]["retry_attempted"].sum():,}')
print(f'Retries that succeeded: {enriched["retry_success"].sum():,}')
print(f'\nUnique payment types found:')
for payment_type in payments['payment_type'].unique():
    count = len(payments[payments['payment_type'] == payment_type])
    print(f'  - {payment_type}: {count:,}')
print(f'\nTotal revenue at risk: load_data.py{enriched["revenue_at_risk"].sum():,.2f}')
print('='*60)
print('\nData loading complete!')
