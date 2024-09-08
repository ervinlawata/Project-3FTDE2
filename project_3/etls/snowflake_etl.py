import pandas as pd
import psycopg2
from snowflake.connector.cursor import SnowflakeCursor,ProgrammingError
import snowflake.connector
import os 
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dag_file_path = os.path.dirname(os.path.abspath(__file__))
from utils.config import *
from utils.creds_sf import *

# PostgreSQL connection setup
db_params = {
        "host": "localhost",
        "port": 5431,
        "database": "airflow_dskola_db",
        "user": "postgres",
        "password": "postgres"
    }
    
# Connect to PostgreSQL
conn_ps = psycopg2.connect(**db_params)

ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA,
    role = ROLE
    )

def tf_dm_supplier_revenue():
    # Extract data from PostgreSQL
    query = """
    WITH supplier_revenue AS (
      SELECT
        s.COMPANYNAME AS company_name,
        TO_DATE(CONCAT(EXTRACT(MONTH FROM o.ORDERDATE), '-', EXTRACT(YEAR FROM o.ORDERDATE)), 'MM-YYYY') AS month_order,
        SUM((od.UNITPRICE - (od.UNITPRICE * od.DISCOUNT)) * od.QUANTITY) AS gross_revenue
      FROM
        fact_order_details od
        JOIN fact_orders o ON od.ORDERID = o.ORDERID
        JOIN dim_products p ON od.PRODUCTID = p.PRODUCTID
        JOIN dim_suppliers s ON p.SUPPLIERID = s.SUPPLIERID
      GROUP BY
        company_name, month_order
    )

    SELECT
      company_name,
      month_order,
      gross_revenue
    FROM
      supplier_revenue;
    """
    df = pd.read_sql_query(query,conn_ps)
    df.to_csv('data/dm_supplier_revenue.csv', index=False)

def tf_dm_top_employee_revenue():
    # Extract data from PostgreSQL
    query ="""
    WITH employee_revenue AS (
      SELECT
        e.FIRSTNAME || ' ' || e.LASTNAME AS employee_name,
        TO_CHAR(DATE_TRUNC('month', o.ORDERDATE), 'YYYY-MM') AS month_order,
        SUM((od.UNITPRICE - (od.UNITPRICE * od.DISCOUNT)) * od.QUANTITY) AS gross_revenue
      FROM
        fact_order_details od
        JOIN fact_orders o ON od.ORDERID = o.ORDERID
        JOIN dim_employees e ON o.EMPLOYEEID = e.EMPLOYEEID
      GROUP BY
        employee_name, month_order
    ),
    ranked_employees AS (
      SELECT
        employee_name,
        month_order,
        gross_revenue,
        ROW_NUMBER() OVER (PARTITION BY month_order ORDER BY gross_revenue DESC) AS rank
      FROM
        employee_revenue
    )

    SELECT
      employee_name,
      month_order,
      gross_revenue
    FROM
      ranked_employees
    WHERE
      rank = 1;
    """

    df = pd.read_sql_query(query,conn_ps)
    df.to_csv('data/dm_top_employee_revenue.csv', index=False)

def tf_dm_top_category_sales():
    # Extract data from PostgreSQL
    query = """
    WITH category_sales AS (
      SELECT
        c.CATEGORYNAME AS category_name,
        TO_CHAR(DATE_TRUNC('month', o.ORDERDATE), 'YYYY-MM') AS month_order,
        SUM(od.QUANTITY) AS total_sold
      FROM
        fact_order_details od
        JOIN fact_orders o ON od.ORDERID = o.ORDERID
        JOIN dim_products p ON od.PRODUCTID = p.PRODUCTID
        JOIN dim_categories c ON p.CATEGORYID = c.CATEGORYID
      GROUP BY
        category_name, month_order
    ),
    ranked_categories AS (
      SELECT
        category_name,
        month_order,
        total_sold,
        ROW_NUMBER() OVER (PARTITION BY month_order ORDER BY total_sold DESC) AS rank
      FROM
        category_sales
    )
    SELECT
      category_name,
      month_order,
      total_sold
    FROM
      ranked_categories
    WHERE
      rank = 1;
    """

    df = pd.read_sql_query(query,conn_ps)
    df.to_csv('data/dm_top_category_sales.csv', index=False)

def create_snowflake_tables(conn, table_creation_queries):
    cursor = ctx.cursor(SnowflakeCursor)  # Specify SnowflakeCursor type
    cursor.execute(f'''use database DWH_AIRFLOW_DSKOLA''') 
    for table_name, creation_query in table_creation_queries.items():
        cur.execute(f"""{creation_query}""")
        print(f"Table {table_name} created or replaced successfully.")
        

def load_csv_to_snowflake_staging():
    cursor = ctx.cursor(SnowflakeCursor)  # Specify SnowflakeCursor type
    cursor.execute(f'''use database DWH_AIRFLOW_DSKOLA''')
    # Get list of CSV files in the directory
    data_dir = "data/"
    csv_files = [f for f in os.listdir(data_dir) if f.startswith("dm")]  #

    for file_name in csv_files:
       
        file_path = os.path.join(data_dir, file_name)

        # Upload data to temporary location
        cursor.execute(f"PUT 'file://{file_path}' @from_postgres auto_compress=true;")
        print(f"Uploaded {file_name} to Snowflake staging")

    # Close the cursor
    cursor.close()
    print("All CSV files processed.")

def load_to_snowflake():

    # Create a Snowflake cursor
    cursor = ctx.cursor(SnowflakeCursor)
    # Paths to CSV files for each data mart
    tables = [
    {"table_name": "DM_MONTHLY_SUPPLIER_GROSS_REVENUE", "pattern": ".*supplier_revenue.*"},
    {"table_name": "DM_TOP_EMPLOYEE_REVENUE", "pattern": ".*employee_revenue.*"},
    {"table_name": "DM_TOP_CATEGORY_SALES", "pattern": ".*category_sales.*"}
    ]

    cursor.execute(f'''use database DWH_AIRFLOW_DSKOLA''')

    # Create tables
    for table_name, creation_query in table_creation_queries.items():
          cursor.execute(f"""{creation_query}""")
          print(f"Table {table_name} created or replaced successfully.")

    # Loop through each table dictionary
    for table in tables:
        table_name = table["table_name"]
        pattern = table["pattern"]

        # Load data using COPY INTO
        try:
            cursor.execute(f"""
                           copy into {table_name} 
                           from (select distinct * from @from_postgres t) 
                           file_format = allow_duplicate_ff 
                           on_error = 'CONTINUE' 
                           pattern='{pattern}'
                           force = false;
                           """) 
            
            print(f"Loaded data into table: {table_name}")
        except snowflake.connector.Error as err:
            print(f"Error encountered for table {table_name}: {err}")

    # Close the cursor
    cursor.close()
    print("All CSV files processed.")





