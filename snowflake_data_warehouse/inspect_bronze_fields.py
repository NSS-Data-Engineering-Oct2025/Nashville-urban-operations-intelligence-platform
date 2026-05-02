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


def inspect_bronze_fields(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    inspection_queries = [
        """
        SELECT 
            'BRONZE_NASHVILLE_311_SERVICE_REQUESTS' AS TABLE_NAME,
            RECORD_DATA:attributes AS SAMPLE_ATTRIBUTES
        FROM BRONZE_NASHVILLE_311_SERVICE_REQUESTS
        LIMIT 1;
        """,
        """
        SELECT 
            'BRONZE_NASHVILLE_HOUSING_PROPERTY_DATA' AS TABLE_NAME,
            RECORD_DATA:attributes AS SAMPLE_ATTRIBUTES
        FROM BRONZE_NASHVILLE_HOUSING_PROPERTY_DATA
        LIMIT 1;
        """,
        """
        SELECT 
            'BRONZE_NASHVILLE_PUBLIC_SAFETY_DATA' AS TABLE_NAME,
            RECORD_DATA:attributes AS SAMPLE_ATTRIBUTES
        FROM BRONZE_NASHVILLE_PUBLIC_SAFETY_DATA
        LIMIT 1;
        """
    ]

    for inspection_query in inspection_queries:
        snowflake_cursor.execute(inspection_query)
        inspection_result = snowflake_cursor.fetchone()

        print("\nTable:")
        print(inspection_result[0])

        print("\nSample attributes:")
        print(inspection_result[1])

        print("\n-----------------------------------")

    snowflake_cursor.close()


if __name__ == "__main__":
    snowflake_connection = create_snowflake_connection()

    inspect_bronze_fields(snowflake_connection)

    snowflake_connection.close()