import pandas as pd
import requests
import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dag_file_path = os.path.dirname(os.path.abspath(__file__))

csv_files = [
    ('categories.csv', 'dim_categories'),
    ('customers.csv', 'dim_customers'),
    ('employee_territories.csv', 'dim_employee_territories'),
    ('employees.csv', 'dim_employees'),
    ('order_details.csv', 'fact_order_details'),
    ('orders.csv', 'fact_orders'),
    ('products.csv', 'dim_products'),
    ('regions.csv', 'dim_regions'),
    ('shippers.csv', 'dim_shippers'),
    ('suppliers.csv', 'dim_suppliers'),
    ('territories.csv', 'dim_territories'),
]
def deduplicate_data(new_data, existing_data, unique_key):
    """Remove duplicates from new data based on existing data."""
    existing_keys = existing_data[unique_key].tolist()
    unique_rows = new_data[~new_data[unique_key].isin(existing_keys)]
    return unique_rows

def get_unique_key(table_name):
    """Retrieve the unique key of the table."""
    table_keys = {
        'dim_categories': 'categoryid',
        'dim_employees': 'employeeid',
        'dim_products': 'productid',
        'dim_suppliers': 'supplierid',
        'fact_orders': 'orderid',
        'fact_order_details': 'detailid',
    }
    if table_name in table_keys:
        return table_keys[table_name]
    else:
        raise ValueError("Table name not recognized.")

# Define the task to extract CSVs from GitHub
def extract_csv():
    base_url = 'https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/'
    for csv_file, _ in csv_files:
        url = f"{base_url}{csv_file}"
        print(url)
        df = pd.read_csv(url)
        df.to_csv(f"data/{csv_file}", index=False)

def load_to_postgres():
    # Define the connection to PostgreSQL

    db_params = {
        "host": "localhost",
        "port": 5431,
        "database": "airflow_dskola_db",
        "user": "postgres",
        "password": "postgres"
    }
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)
    # List of dictionaries containing table names and file patterns
    tables = [
        {"table_name": "dim_categories", "csv_file": "categories.csv"},
        {"table_name": "dim_employees", "csv_file": "employees.csv"},
        {"table_name": "dim_suppliers", "csv_file": "suppliers.csv"},
        {"table_name": "dim_products", "csv_file": "products.csv"},
        {"table_name": "fact_orders", "csv_file": "orders.csv"},
        {"table_name": "fact_order_details", "csv_file": "order_details.csv"},
    ]
    
    # Loop through each table dictionary
    for table in tables:
        table_name = table["table_name"]
        csv_file = table["csv_file"]
        print(table_name)
        try:
            # Read the CSV file
            df = pd.read_csv(f"data/{csv_file}")
            df.columns = df.columns.str.lower()
            # Get existing data from PostgreSQL
            #existing_data = pd.read_sql_table(table_name, engine)
            query = f"SELECT * FROM public.{table_name}"
            existing_data = pd.read_sql(query, conn)
            # Get unique key for the table
            unique_key = get_unique_key(table_name)
            # Handle deduplication using single key
            
            if table_name == 'fact_order_details':
                # Generate unique DETAILID
                if 'detailid' not in df.columns:
                    existing_max_detailid = existing_data['detailid'].max() if not existing_data.empty else 0
                    df['detailid'] = range(existing_max_detailid + 1, existing_max_detailid + len(df) + 1)
                # Ensure no duplicate rows based on orderid and productid
                existing_keys = existing_data[['orderid', 'productid']]
                new_keys = df[['orderid', 'productid']]
                merged_keys = pd.merge(new_keys, existing_keys, how='left', indicator=True)
                unique_data = df[merged_keys['_merge'] == 'left_only']
                unique_data = unique_data[['detailid', 'orderid', 'productid', 'unitprice', 'quantity', 'discount']]
            else:
                # Handle deduplication using single key
                unique_data = deduplicate_data(df, existing_data, unique_key)
            
            print(unique_data.head(5))
            with conn.cursor() as cur:
                for row in unique_data.itertuples(index=False, name=None):
                    placeholders = ', '.join(['%s'] * len(row))
                    insert_query = f"INSERT INTO public.{table_name} VALUES ({placeholders})"
                    cur.execute(insert_query, row)
                conn.commit()
        except (FileNotFoundError, pd.errors.ParserError) as file_err:
            print(f"Error reading CSV file {csv_file}: {file_err}")
        except SQLAlchemyError as db_err:
            print(f"Error inserting data into table {table_name}: {db_err}")

    print("All CSV files processed.")