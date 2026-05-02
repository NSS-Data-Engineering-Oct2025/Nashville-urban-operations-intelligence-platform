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


def create_gold_tables(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    create_311_requests_by_type_table_query = """
    CREATE OR REPLACE TABLE GOLD_311_REQUESTS_BY_TYPE AS
    SELECT
        REQUEST_TYPE,
        COUNT(*) AS TOTAL_REQUESTS
    FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
    WHERE REQUEST_TYPE IS NOT NULL
    GROUP BY REQUEST_TYPE
    ORDER BY TOTAL_REQUESTS DESC;
    """

    create_311_requests_by_status_table_query = """
    CREATE OR REPLACE TABLE GOLD_311_REQUESTS_BY_STATUS AS
    SELECT
        STATUS,
        COUNT(*) AS TOTAL_REQUESTS
    FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
    WHERE STATUS IS NOT NULL
    GROUP BY STATUS
    ORDER BY TOTAL_REQUESTS DESC;
    """

    create_housing_by_land_use_table_query = """
    CREATE OR REPLACE TABLE GOLD_HOUSING_BY_LAND_USE AS
    SELECT
        LAND_USE_DESCRIPTION,
        COUNT(*) AS TOTAL_PROPERTIES,
        AVG(TOTAL_APPRAISED_VALUE) AS AVERAGE_APPRAISED_VALUE,
        MIN(TOTAL_APPRAISED_VALUE) AS MINIMUM_APPRAISED_VALUE,
        MAX(TOTAL_APPRAISED_VALUE) AS MAXIMUM_APPRAISED_VALUE
    FROM SILVER_NASHVILLE_HOUSING_PROPERTY_DATA
    WHERE LAND_USE_DESCRIPTION IS NOT NULL
    GROUP BY LAND_USE_DESCRIPTION
    ORDER BY TOTAL_PROPERTIES DESC;
    """

    create_housing_by_city_table_query = """
    CREATE OR REPLACE TABLE GOLD_HOUSING_BY_CITY AS
    SELECT
        PROPERTY_CITY,
        COUNT(*) AS TOTAL_PROPERTIES,
        AVG(TOTAL_APPRAISED_VALUE) AS AVERAGE_APPRAISED_VALUE
    FROM SILVER_NASHVILLE_HOUSING_PROPERTY_DATA
    WHERE PROPERTY_CITY IS NOT NULL
    GROUP BY PROPERTY_CITY
    ORDER BY TOTAL_PROPERTIES DESC;
    """

    create_public_safety_by_city_table_query = """
    CREATE OR REPLACE TABLE GOLD_PUBLIC_SAFETY_BY_CITY AS
    SELECT
        CITY,
        COUNT(*) AS TOTAL_PUBLIC_SAFETY_LOCATIONS
    FROM SILVER_NASHVILLE_PUBLIC_SAFETY_DATA
    WHERE CITY IS NOT NULL
    GROUP BY CITY
    ORDER BY TOTAL_PUBLIC_SAFETY_LOCATIONS DESC;
    """

    snowflake_cursor.execute(create_311_requests_by_type_table_query)
    print("GOLD_311_REQUESTS_BY_TYPE table created")

    snowflake_cursor.execute(create_311_requests_by_status_table_query)
    print("GOLD_311_REQUESTS_BY_STATUS table created")

    snowflake_cursor.execute(create_housing_by_land_use_table_query)
    print("GOLD_HOUSING_BY_LAND_USE table created")

    snowflake_cursor.execute(create_housing_by_city_table_query)
    print("GOLD_HOUSING_BY_CITY table created")

    snowflake_cursor.execute(create_public_safety_by_city_table_query)
    print("GOLD_PUBLIC_SAFETY_BY_CITY table created")

    snowflake_cursor.close()


def preview_gold_tables(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    preview_queries = [
        "SELECT * FROM GOLD_311_REQUESTS_BY_TYPE LIMIT 5;",
        "SELECT * FROM GOLD_311_REQUESTS_BY_STATUS LIMIT 5;",
        "SELECT * FROM GOLD_HOUSING_BY_LAND_USE LIMIT 5;",
        "SELECT * FROM GOLD_HOUSING_BY_CITY LIMIT 5;",
        "SELECT * FROM GOLD_PUBLIC_SAFETY_BY_CITY LIMIT 5;"
    ]

    print("\nGold table previews:")

    for preview_query in preview_queries:
        snowflake_cursor.execute(preview_query)
        preview_results = snowflake_cursor.fetchall()

        for row in preview_results:
            print(row)

        print("------")

    snowflake_cursor.close()


if __name__ == "__main__":
    snowflake_connection = create_snowflake_connection()

    create_gold_tables(snowflake_connection)
    preview_gold_tables(snowflake_connection)

    snowflake_connection.close()