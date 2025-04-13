from dotenv import load_dotenv
import os
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark import functions as SF
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType, IntegerType, DateType

load_dotenv()
SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_ACCOUNT = os.getenv("SF_ACCOUNT")


db = "PANUCCIS_PIZZA"
schema = "POS_MAIN"
WH = "PANUCCIS_PIZZA_WH"
table_stores = "STORES"
table_customers = "CUSTOMERS"
table_orders = "ORDERS_HEADER"


connection_parameters = {
  "account": SF_ACCOUNT,
  "user": SF_USER,
  "password": SF_PASSWORD,
}

def create_session():
    """
    Creates a Snowflake session using the provided connection parameters.
    """
    return Session.builder.configs(connection_parameters).create()


def setup_snowflake_panuccis_project(session):
    """
    Sets up the Snowflake environment for the Panuccis Pizza project.
    Creates a database, schema, and warehouse if they do not already exist.
    """    
    # Create the Panuccis Pizza Warehouse
    session.sql(f"""
                    CREATE WAREHOUSE if not exists {WH}
                    WITH
                    WAREHOUSE_SIZE = XSMALL
                    AUTO_SUSPEND = 30
                    INITIALLY_SUSPENDED = TRUE
                    COMMENT = 'WH TO DO TEST RUNS ON PANUCCIS PIZZA DB'
                    """).collect()
    session.use_warehouse(f"{WH}")

    # Create the Panuccis Pizza Database
    session.sql(f'CREATE OR REPLACE DATABASE {db};').collect()

    # Create the Panuccis PoS Main Schema
    session.sql(f'CREATE OR REPLACE SCHEMA {db}.{schema};').collect()


def upload_data(session, df, data_schema, db, schema, table_name):
    """
    Uploads the DataFrame to Snowflake.
    """
    # Create a Snowpark DataFrame from the pandas DataFrame
    snowpark_df = session.create_dataframe(df.to_records(index=False).tolist(), schema=data_schema)
    
    # Write the data to the Snowflake table
    snowpark_df.write.mode("overwrite").save_as_table(f"{db}.{schema}.{table_name}")



def upload_customer_data(session, customer_df, db=db, schema=schema, table_customers=table_customers):
    """
    Uploads the store DataFrame to Snowflake.
    """

    # Define the schema for the customer data
    data_schema = StructType([
        StructField("CUSTOMER_ID", StringType()),
        StructField("LATITUDE", FloatType()),
        StructField("LONGITUDE", FloatType()),
        StructField("FIRSTNAME", StringType()),
        StructField("LASTNAME", StringType()),
        StructField("HOMESTORE_ID", IntegerType())
    ])
    upload_data(session, customer_df, data_schema, db, schema, table_customers)


def upload_store_data(session, store_df, db=db, schema=schema, table_stores=table_stores):
    # Define the schema for the store data
    data_schema = StructType([
        StructField("STORE_ID", IntegerType()),
        StructField("LATITUDE", FloatType()),
        StructField("LONGITUDE", FloatType()),
        StructField("STORE_NAME", StringType()),
        StructField("OPENDT", DateType())
    ])
    upload_data(session, store_df, data_schema, db, schema, table_stores)



def upload_order_data(session, order_df, db=db, schema=schema, table_orders=table_orders):
    # Define the schema for the orders data
    data_schema = StructType([
        StructField("ORDER_ID", StringType()),
        StructField("CUSTOMER_ID", StringType()),
        StructField("STORE_ID", IntegerType()),
        StructField("ORDER_DATE", DateType()),
        StructField("SUBTOTAL", FloatType()),
        StructField("TAX", FloatType()),
        StructField("TOTAL", FloatType()),
    ])
    upload_data(session, order_df, data_schema, db, schema, table_orders)


def clean_all_data(session):
    """
    Cleans all data from the Snowflake tables.
    """
    session.sql(f"DROP TABLE IF EXISTS {db}.{schema}.{table_stores};").collect()
    session.sql(f"DROP TABLE IF EXISTS {db}.{schema}.{table_customers};").collect()
    session.sql(f"DROP TABLE IF EXISTS {db}.{schema}.{table_orders};").collect()

    # Drop the database and schema
    session.sql(f"DROP SCHEMA IF EXISTS {db}.{schema}").collect()
    session.sql(f"DROP DATABASE IF EXISTS {db};").collect()

    # Drop the warehouse
    session.sql(f"DROP WAREHOUSE IF EXISTS {WH};").collect()


# Test functions
if __name__ == "__main__":

    session = create_session()
    # Check if the session is created successfully
    if session:
        print("Session created successfully!")
    else:
        print("Failed to create session.")



    # Setup Snowflake environment
    setup_snowflake_panuccis_project(session)

    # Create a sample DataFrame for stores
    store_data = {
        "STORE_ID": [1, 2],
        "LATITUDE": [40.7128, 34.0522],
        "LONGITUDE": [-74.0060, -118.2437],
        "STORE_NAME": ["Store A", "Store B"],
        "OPENDT": ["2023-01-01", "2023-01-02"]
    }
    store_df = pd.DataFrame(store_data)

    # Upload store data to Snowflake
    upload_store_data(session, store_df)

    # Create a sample DataFrame for customers
    customer_data = {
        "CUSTOMER_ID": ["C001", "C002"],
        "LATITUDE": [40.7128, 34.0522],
        "LONGITUDE": [-74.0060, -118.2437],
        "FIRSTNAME": ["John", "Jane"],
        "LASTNAME": ["Doe", "Smith"],
        "HOMESTORE_ID": [1, 2]
    }
    customer_df = pd.DataFrame(customer_data)

    # Upload customer data to Snowflake
    upload_customer_data(session, customer_df)

    # Create a sample DataFrame for orders
    order_data = {
        "ORDER_ID": ["O001", "O002"],
        "CUSTOMER_ID": ["C001", "C002"],
        "STORE_ID": [1, 2],
        "ORDER_DATE": ["2023-01-03", "2023-01-04"],
        "SUBTOTAL": [100.00, 200.00],
        "TAX": [10.00, 20.00],
        "TOTAL": [110.00, 220.00]
    }
    order_df = pd.DataFrame(order_data)

    # Upload order data to Snowflake
    upload_order_data(session, order_df)


    
    # clean_all_data(session)