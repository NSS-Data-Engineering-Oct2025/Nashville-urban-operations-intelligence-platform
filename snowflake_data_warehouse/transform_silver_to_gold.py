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

    create_311_requests_by_type_query = """
    CREATE OR REPLACE TABLE GOLD_311_REQUESTS_BY_TYPE AS
    SELECT
        REQUEST_TYPE,
        COUNT(*) AS TOTAL_REQUESTS
    FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
    WHERE REQUEST_TYPE IS NOT NULL
      AND TRIM(REQUEST_TYPE) <> ''
    GROUP BY REQUEST_TYPE
    ORDER BY TOTAL_REQUESTS DESC;
    """

    create_311_requests_by_status_query = """
    CREATE OR REPLACE TABLE GOLD_311_REQUESTS_BY_STATUS AS
    SELECT
        STATUS,
        COUNT(*) AS TOTAL_REQUESTS
    FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
    WHERE STATUS IS NOT NULL
      AND TRIM(STATUS) <> ''
    GROUP BY STATUS
    ORDER BY TOTAL_REQUESTS DESC;
    """

    create_311_requests_by_city_type_query = """
    CREATE OR REPLACE TABLE GOLD_311_REQUESTS_BY_CITY_TYPE AS
    SELECT
        UPPER(TRIM(CITY)) AS CITY,
        REQUEST_TYPE,
        COUNT(*) AS TOTAL_REQUESTS
    FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
    WHERE CITY IS NOT NULL
      AND TRIM(CITY) <> ''
      AND REQUEST_TYPE IS NOT NULL
      AND TRIM(REQUEST_TYPE) <> ''
    GROUP BY UPPER(TRIM(CITY)), REQUEST_TYPE
    ORDER BY CITY, TOTAL_REQUESTS DESC;
    """

    create_311_requests_by_city_status_query = """
    CREATE OR REPLACE TABLE GOLD_311_REQUESTS_BY_CITY_STATUS AS
    SELECT
        UPPER(TRIM(CITY)) AS CITY,
        STATUS,
        COUNT(*) AS TOTAL_REQUESTS
    FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
    WHERE CITY IS NOT NULL
      AND TRIM(CITY) <> ''
      AND STATUS IS NOT NULL
      AND TRIM(STATUS) <> ''
    GROUP BY UPPER(TRIM(CITY)), STATUS
    ORDER BY CITY, TOTAL_REQUESTS DESC;
    """

    create_property_violations_by_subrequest_type_query = """
    CREATE OR REPLACE TABLE GOLD_PROPERTY_VIOLATIONS_BY_SUBREQUEST_TYPE AS
    SELECT
        SUBREQUEST_TYPE,
        COUNT(*) AS TOTAL_REQUESTS
    FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
    WHERE UPPER(TRIM(REQUEST_TYPE)) LIKE '%PROPERTY VIOLATION%'
      AND SUBREQUEST_TYPE IS NOT NULL
      AND TRIM(SUBREQUEST_TYPE) <> ''
    GROUP BY SUBREQUEST_TYPE
    ORDER BY TOTAL_REQUESTS DESC;
    """

    create_housing_by_city_query = """
    CREATE OR REPLACE TABLE GOLD_HOUSING_BY_CITY AS
    SELECT
        UPPER(TRIM(PROPERTY_CITY)) AS PROPERTY_CITY,
        COUNT(*) AS TOTAL_PROPERTIES,
        AVG(TOTAL_APPRAISED_VALUE) AS AVERAGE_APPRAISED_VALUE
    FROM SILVER_NASHVILLE_HOUSING_PROPERTY_DATA
    WHERE PROPERTY_CITY IS NOT NULL
      AND TRIM(PROPERTY_CITY) <> ''
    GROUP BY UPPER(TRIM(PROPERTY_CITY))
    ORDER BY TOTAL_PROPERTIES DESC;
    """

    create_housing_by_land_use_query = """
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
    """

    create_housing_by_city_land_use_query = """
    CREATE OR REPLACE TABLE GOLD_HOUSING_BY_CITY_LAND_USE AS
    SELECT
        UPPER(TRIM(PROPERTY_CITY)) AS PROPERTY_CITY,
        LAND_USE_DESCRIPTION,
        COUNT(*) AS TOTAL_PROPERTIES,
        AVG(TOTAL_APPRAISED_VALUE) AS AVERAGE_APPRAISED_VALUE
    FROM SILVER_NASHVILLE_HOUSING_PROPERTY_DATA
    WHERE PROPERTY_CITY IS NOT NULL
      AND TRIM(PROPERTY_CITY) <> ''
      AND LAND_USE_DESCRIPTION IS NOT NULL
      AND TRIM(LAND_USE_DESCRIPTION) <> ''
    GROUP BY UPPER(TRIM(PROPERTY_CITY)), LAND_USE_DESCRIPTION
    ORDER BY PROPERTY_CITY, TOTAL_PROPERTIES DESC;
    """

    create_city_operations_summary_query = """
    CREATE OR REPLACE TABLE GOLD_CITY_OPERATIONS_SUMMARY AS
    SELECT
        COALESCE(r.CITY, h.PROPERTY_CITY) AS CITY,

        COALESCE(r.TOTAL_311_REQUESTS, 0) AS TOTAL_311_REQUESTS,
        COALESCE(r.TOTAL_PROPERTY_VIOLATIONS, 0) AS TOTAL_PROPERTY_VIOLATIONS,

        COALESCE(h.TOTAL_PROPERTIES, 0) AS TOTAL_PROPERTIES,
        COALESCE(h.AVERAGE_APPRAISED_VALUE, 0) AS AVERAGE_APPRAISED_VALUE

    FROM
    (
        SELECT
            UPPER(TRIM(CITY)) AS CITY,
            COUNT(*) AS TOTAL_311_REQUESTS,
            SUM(
                CASE
                    WHEN UPPER(TRIM(REQUEST_TYPE)) LIKE '%PROPERTY VIOLATION%'
                    THEN 1
                    ELSE 0
                END
            ) AS TOTAL_PROPERTY_VIOLATIONS
        FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
        WHERE CITY IS NOT NULL
          AND TRIM(CITY) <> ''
        GROUP BY UPPER(TRIM(CITY))
    ) r

    FULL OUTER JOIN
    (
        SELECT
            UPPER(TRIM(PROPERTY_CITY)) AS PROPERTY_CITY,
            COUNT(*) AS TOTAL_PROPERTIES,
            AVG(TOTAL_APPRAISED_VALUE) AS AVERAGE_APPRAISED_VALUE
        FROM SILVER_NASHVILLE_HOUSING_PROPERTY_DATA
        WHERE PROPERTY_CITY IS NOT NULL
          AND TRIM(PROPERTY_CITY) <> ''
        GROUP BY UPPER(TRIM(PROPERTY_CITY))
    ) h

    ON r.CITY = h.PROPERTY_CITY

    WHERE COALESCE(r.CITY, h.PROPERTY_CITY) IS NOT NULL
    ORDER BY CITY;
    """

    gold_queries = [
        ("GOLD_311_REQUESTS_BY_TYPE", create_311_requests_by_type_query),
        ("GOLD_311_REQUESTS_BY_STATUS", create_311_requests_by_status_query),
        ("GOLD_311_REQUESTS_BY_CITY_TYPE", create_311_requests_by_city_type_query),
        ("GOLD_311_REQUESTS_BY_CITY_STATUS", create_311_requests_by_city_status_query),
        ("GOLD_PROPERTY_VIOLATIONS_BY_SUBREQUEST_TYPE", create_property_violations_by_subrequest_type_query),
        ("GOLD_HOUSING_BY_CITY", create_housing_by_city_query),
        ("GOLD_HOUSING_BY_LAND_USE", create_housing_by_land_use_query),
        ("GOLD_HOUSING_BY_CITY_LAND_USE", create_housing_by_city_land_use_query),
        ("GOLD_CITY_OPERATIONS_SUMMARY", create_city_operations_summary_query)
    ]

    for table_name, query in gold_queries:
        snowflake_cursor.execute(query)
        print(f"{table_name} table created")

    snowflake_cursor.close()


def preview_gold_tables(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    preview_queries = [
        "SELECT * FROM GOLD_CITY_OPERATIONS_SUMMARY ORDER BY TOTAL_311_REQUESTS DESC, TOTAL_PROPERTIES DESC LIMIT 10;",
        "SELECT * FROM GOLD_311_REQUESTS_BY_TYPE LIMIT 5;",
        "SELECT * FROM GOLD_HOUSING_BY_CITY LIMIT 5;"
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