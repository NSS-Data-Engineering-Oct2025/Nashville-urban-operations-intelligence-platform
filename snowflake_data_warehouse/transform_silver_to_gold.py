import os
from typing import Dict

import snowflake.connector
from dotenv import load_dotenv


load_dotenv()


def create_snowflake_connection():
    """Create and return a Snowflake database connection."""
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


def get_gold_table_queries() -> Dict[str, str]:
    """Return SQL queries used to create gold-level reporting tables."""
    return {
        "GOLD_311_REQUESTS_BY_TYPE": """
            CREATE OR REPLACE TABLE GOLD_311_REQUESTS_BY_TYPE AS
            SELECT
                REQUEST_TYPE,
                COUNT(*) AS TOTAL_REQUESTS
            FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
            WHERE REQUEST_TYPE IS NOT NULL
            GROUP BY REQUEST_TYPE
            ORDER BY TOTAL_REQUESTS DESC;
        """,

        "GOLD_311_REQUESTS_BY_STATUS": """
            CREATE OR REPLACE TABLE GOLD_311_REQUESTS_BY_STATUS AS
            SELECT
                STATUS,
                COUNT(*) AS TOTAL_REQUESTS
            FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
            WHERE STATUS IS NOT NULL
            GROUP BY STATUS
            ORDER BY TOTAL_REQUESTS DESC;
        """,

        "GOLD_311_REQUESTS_BY_CITY_TYPE": """
            CREATE OR REPLACE TABLE GOLD_311_REQUESTS_BY_CITY_TYPE AS
            SELECT
                CITY,
                REQUEST_TYPE,
                COUNT(*) AS TOTAL_REQUESTS
            FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
            WHERE CITY IS NOT NULL
              AND TRIM(CITY) <> ''
              AND REQUEST_TYPE IS NOT NULL
            GROUP BY CITY, REQUEST_TYPE
            ORDER BY CITY, TOTAL_REQUESTS DESC;
        """,

        "GOLD_311_REQUESTS_BY_CITY_STATUS": """
            CREATE OR REPLACE TABLE GOLD_311_REQUESTS_BY_CITY_STATUS AS
            SELECT
                CITY,
                STATUS,
                COUNT(*) AS TOTAL_REQUESTS
            FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
            WHERE CITY IS NOT NULL
              AND TRIM(CITY) <> ''
              AND STATUS IS NOT NULL
            GROUP BY CITY, STATUS
            ORDER BY CITY, TOTAL_REQUESTS DESC;
        """,

        "GOLD_PROPERTY_VIOLATIONS_BY_CITY": """
            CREATE OR REPLACE TABLE GOLD_PROPERTY_VIOLATIONS_BY_CITY AS
            SELECT
                CITY,
                COUNT(*) AS TOTAL_REQUESTS
            FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
            WHERE REQUEST_TYPE = 'Property Violations'
              AND CITY IS NOT NULL
              AND TRIM(CITY) <> ''
            GROUP BY CITY
            ORDER BY TOTAL_REQUESTS DESC;
        """,

        "GOLD_PROPERTY_VIOLATIONS_BY_SUBREQUEST_TYPE": """
            CREATE OR REPLACE TABLE GOLD_PROPERTY_VIOLATIONS_BY_SUBREQUEST_TYPE AS
            SELECT
                SUBREQUEST_TYPE,
                COUNT(*) AS TOTAL_REQUESTS
            FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
            WHERE REQUEST_TYPE = 'Property Violations'
              AND SUBREQUEST_TYPE IS NOT NULL
              AND TRIM(SUBREQUEST_TYPE) <> ''
            GROUP BY SUBREQUEST_TYPE
            ORDER BY TOTAL_REQUESTS DESC;
        """,

        "GOLD_PROPERTY_VIOLATIONS_BY_CITY_TYPE": """
            CREATE OR REPLACE TABLE GOLD_PROPERTY_VIOLATIONS_BY_CITY_TYPE AS
            SELECT
                CITY,
                SUBREQUEST_TYPE,
                COUNT(*) AS TOTAL_REQUESTS
            FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
            WHERE REQUEST_TYPE = 'Property Violations'
              AND CITY IS NOT NULL
              AND TRIM(CITY) <> ''
              AND SUBREQUEST_TYPE IS NOT NULL
              AND TRIM(SUBREQUEST_TYPE) <> ''
            GROUP BY CITY, SUBREQUEST_TYPE
            ORDER BY CITY, TOTAL_REQUESTS DESC;
        """,

        "GOLD_HOUSING_BY_CITY": """
            CREATE OR REPLACE TABLE GOLD_HOUSING_BY_CITY AS
            SELECT
                PROPERTY_CITY,
                COUNT(*) AS TOTAL_PROPERTIES,
                AVG(TOTAL_APPRAISED_VALUE) AS AVERAGE_APPRAISED_VALUE
            FROM SILVER_NASHVILLE_HOUSING_PROPERTY_DATA
            WHERE PROPERTY_CITY IS NOT NULL
              AND TRIM(PROPERTY_CITY) <> ''
            GROUP BY PROPERTY_CITY
            ORDER BY TOTAL_PROPERTIES DESC;
        """,

        "GOLD_HOUSING_BY_LAND_USE": """
            CREATE OR REPLACE TABLE GOLD_HOUSING_BY_LAND_USE AS
            SELECT
                LAND_USE_DESCRIPTION,
                COUNT(*) AS TOTAL_PROPERTIES,
                AVG(TOTAL_APPRAISED_VALUE) AS AVERAGE_APPRAISED_VALUE,
                MIN(TOTAL_APPRAISED_VALUE) AS MINIMUM_APPRAISED_VALUE,
                MAX(TOTAL_APPRAISED_VALUE) AS MAXIMUM_APPRAISED_VALUE
            FROM SILVER_NASHVILLE_HOUSING_PROPERTY_DATA
            WHERE LAND_USE_DESCRIPTION IS NOT NULL
              AND TRIM(LAND_USE_DESCRIPTION) <> ''
            GROUP BY LAND_USE_DESCRIPTION
            ORDER BY TOTAL_PROPERTIES DESC;
        """,

        "GOLD_HOUSING_BY_CITY_LAND_USE": """
            CREATE OR REPLACE TABLE GOLD_HOUSING_BY_CITY_LAND_USE AS
            SELECT
                PROPERTY_CITY,
                LAND_USE_DESCRIPTION,
                COUNT(*) AS TOTAL_PROPERTIES,
                AVG(TOTAL_APPRAISED_VALUE) AS AVERAGE_APPRAISED_VALUE
            FROM SILVER_NASHVILLE_HOUSING_PROPERTY_DATA
            WHERE PROPERTY_CITY IS NOT NULL
              AND TRIM(PROPERTY_CITY) <> ''
              AND LAND_USE_DESCRIPTION IS NOT NULL
              AND TRIM(LAND_USE_DESCRIPTION) <> ''
            GROUP BY PROPERTY_CITY, LAND_USE_DESCRIPTION
            ORDER BY PROPERTY_CITY, TOTAL_PROPERTIES DESC;
        """,
    }


def create_gold_tables(connection) -> None:
    """Create all gold-level tables in Snowflake."""
    queries = get_gold_table_queries()

    with connection.cursor() as cursor:
        for table_name, query in queries.items():
            try:
                cursor.execute(query)
                print(f"Successfully created table: {table_name}")
            except Exception as error:
                print(f"Failed to create table: {table_name}")
                print(f"Error: {error}")


def main() -> None:
    """Main function to run the gold table creation process."""
    connection = None

    try:
        connection = create_snowflake_connection()
        create_gold_tables(connection)
        print("Gold table creation process completed.")

    except Exception as error:
        print("Snowflake process failed.")
        print(f"Error: {error}")

    finally:
        if connection:
            connection.close()
            print("Snowflake connection closed.")


if __name__ == "__main__":
    main()