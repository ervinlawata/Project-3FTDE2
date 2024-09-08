table_creation_queries = {
    'supplier_revenue': """
        CREATE OR REPLACE TABLE DM_MONTHLY_SUPPLIER_GROSS_REVENUE (
            COMPANY_NAME VARCHAR(16777216),
            MONTH_ORDER VARCHAR(16777216),
            GROSS_REVENUE NUMBER(38,2)
        )
    """,
    'top_employee_revenue': """
        CREATE OR REPLACE TABLE DM_TOP_EMPLOYEE_REVENUE (
            EMPLOYEE_NAME VARCHAR(16777216),
            MONTH_ORDER VARCHAR(16777216),
            GROSS_REVENUE NUMBER(38,2)
        )
    """,
    'top_category_sales': """
        CREATE OR REPLACE TABLE DM_TOP_CATEGORY_SALES (
            CATEGORY_NAME VARCHAR(16777216),
            MONTH_ORDER VARCHAR(16777216),
            TOTAL_SOLD NUMBER(38,2)
        )
    """
}