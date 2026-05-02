import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def create_snowflake_connection():
    snowflake_connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    return snowflake_connection


def create_bronze_tables(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    create_311_bronze_table = """
    CREATE OR REPLACE TABLE BRONZE_NASHVILLE_311_SERVICE_REQUESTS AS
    SELECT
        VALUE AS RECORD_DATA
    FROM RAW_NASHVILLE_311_SERVICE_REQUESTS,
         LATERAL FLATTEN(INPUT => RAW_DATA:features);
    """

    create_housing_bronze_table = """
    CREATE OR REPLACE TABLE BRONZE_NASHVILLE_HOUSING_PROPERTY_DATA AS
    SELECT
        VALUE AS RECORD_DATA
    FROM RAW_NASHVILLE_HOUSING_PROPERTY_DATA,
         LATERAL FLATTEN(INPUT => RAW_DATA:features);
    """

    create_public_safety_bronze_table = """
    CREATE OR REPLACE TABLE BRONZE_NASHVILLE_PUBLIC_SAFETY_DATA AS
    SELECT
        VALUE AS RECORD_DATA
    FROM RAW_NASHVILLE_PUBLIC_SAFETY_DATA,
         LATERAL FLATTEN(INPUT => RAW_DATA:features);
    """

    snowflake_cursor.execute(create_311_bronze_table)
    print("BRONZE 311 table created")

    snowflake_cursor.execute(create_housing_bronze_table)
    print("BRONZE housing table created")

    snowflake_cursor.execute(create_public_safety_bronze_table)
    print("BRONZE public safety table created")

    snowflake_cursor.close()


def check_bronze_counts(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    count_queries = [
        "SELECT 'BRONZE_311' AS TABLE_NAME, COUNT(*) FROM BRONZE_NASHVILLE_311_SERVICE_REQUESTS;",
        "SELECT 'BRONZE_HOUSING' AS TABLE_NAME, COUNT(*) FROM BRONZE_NASHVILLE_HOUSING_PROPERTY_DATA;",
        "SELECT 'BRONZE_PUBLIC_SAFETY' AS TABLE_NAME, COUNT(*) FROM BRONZE_NASHVILLE_PUBLIC_SAFETY_DATA;"
    ]

    print("\nBronze table record counts:")

    for query in count_queries:
        snowflake_cursor.execute(query)
        result = snowflake_cursor.fetchone()
        print(result)

    snowflake_cursor.close()


if __name__ == "__main__":
    snowflake_connection = create_snowflake_connection()

    create_bronze_tables(snowflake_connection)
    check_bronze_counts(snowflake_connection)

    snowflake_connection.close()