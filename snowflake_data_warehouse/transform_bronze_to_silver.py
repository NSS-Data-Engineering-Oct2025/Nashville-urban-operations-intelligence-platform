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


def create_silver_tables(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    create_311_silver_query = """
    CREATE OR REPLACE TABLE SILVER_NASHVILLE_311_SERVICE_REQUESTS AS
    SELECT
        RAW_DATA:attributes.Request__::STRING AS REQUEST_ID,
        RAW_DATA:attributes.Request_Type::STRING AS REQUEST_TYPE,
        RAW_DATA:attributes.Subrequest_Type::STRING AS SUBREQUEST_TYPE,
        RAW_DATA:attributes.Status::STRING AS STATUS,
        RAW_DATA:attributes.City::STRING AS CITY,
        RAW_DATA:attributes.ZIP::STRING AS ZIP_CODE,
        RAW_DATA:attributes.Council_District::STRING AS COUNCIL_DISTRICT,
        RAW_DATA:attributes.Latitude::FLOAT AS LATITUDE,
        RAW_DATA:attributes.Longitude::FLOAT AS LONGITUDE,
        TO_TIMESTAMP_NTZ(RAW_DATA:attributes.Date_Time_Opened::NUMBER / 1000) AS DATE_TIME_OPENED,
        TO_TIMESTAMP_NTZ(RAW_DATA:attributes.Date_Time_Closed::NUMBER / 1000) AS DATE_TIME_CLOSED,
        SOURCE_FILE_NAME,
        LOADED_AT
    FROM RAW_NASHVILLE_311_SERVICE_REQUESTS,
    LATERAL FLATTEN(INPUT => RAW_DATA:features);
    """

    create_housing_silver_query = """
    CREATE OR REPLACE TABLE SILVER_NASHVILLE_HOUSING_PROPERTY_DATA AS
    SELECT
        RAW_DATA:attributes.APN::STRING AS PARCEL_ID,
        RAW_DATA:attributes.Owner::STRING AS OWNER_NAME,
        RAW_DATA:attributes.PropAddr::STRING AS PROPERTY_ADDRESS,
        RAW_DATA:attributes.PropCity::STRING AS PROPERTY_CITY,
        RAW_DATA:attributes.PropState::STRING AS PROPERTY_STATE,
        RAW_DATA:attributes.PropZip::STRING AS PROPERTY_ZIP_CODE,
        RAW_DATA:attributes.LUDesc::STRING AS LAND_USE_DESCRIPTION,
        RAW_DATA:attributes.TotlAppr::FLOAT AS TOTAL_APPRAISED_VALUE,
        RAW_DATA:attributes.LandAppr::FLOAT AS LAND_APPRAISED_VALUE,
        RAW_DATA:attributes.ImprAppr::FLOAT AS IMPROVEMENT_APPRAISED_VALUE,
        RAW_DATA:attributes.Acres::FLOAT AS ACRES,
        RAW_DATA:attributes.SalePrice::FLOAT AS SALE_PRICE,
        SOURCE_FILE_NAME,
        LOADED_AT
    FROM RAW_NASHVILLE_HOUSING_PROPERTY_DATA,
    LATERAL FLATTEN(INPUT => RAW_DATA:features);
    """

    snowflake_cursor.execute(create_311_silver_query)
    print("SILVER_NASHVILLE_311_SERVICE_REQUESTS table created")

    snowflake_cursor.execute(create_housing_silver_query)
    print("SILVER_NASHVILLE_HOUSING_PROPERTY_DATA table created")

    snowflake_cursor.close()


def preview_silver_tables(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    preview_queries = [
        """
        SELECT
            REQUEST_ID,
            REQUEST_TYPE,
            SUBREQUEST_TYPE,
            STATUS,
            CITY
        FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS
        LIMIT 5;
        """,
        """
        SELECT
            PARCEL_ID,
            OWNER_NAME,
            PROPERTY_CITY,
            LAND_USE_DESCRIPTION,
            TOTAL_APPRAISED_VALUE
        FROM SILVER_NASHVILLE_HOUSING_PROPERTY_DATA
        LIMIT 5;
        """
    ]

    print("\nSilver table previews:")

    for preview_query in preview_queries:
        snowflake_cursor.execute(preview_query)
        preview_results = snowflake_cursor.fetchall()

        for row in preview_results:
            print(row)

        print("------")

    snowflake_cursor.close()


def check_silver_record_counts(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    record_count_queries = [
        """
        SELECT
            'SILVER_NASHVILLE_311_SERVICE_REQUESTS' AS TABLE_NAME,
            COUNT(*) AS RECORD_COUNT
        FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS;
        """,
        """
        SELECT
            'SILVER_NASHVILLE_HOUSING_PROPERTY_DATA' AS TABLE_NAME,
            COUNT(*) AS RECORD_COUNT
        FROM SILVER_NASHVILLE_HOUSING_PROPERTY_DATA;
        """
    ]

    print("\nSilver table record counts:")

    for record_count_query in record_count_queries:
        snowflake_cursor.execute(record_count_query)
        record_count_result = snowflake_cursor.fetchone()
        print(record_count_result)

    snowflake_cursor.close()


if __name__ == "__main__":
    snowflake_connection = create_snowflake_connection()

    create_silver_tables(snowflake_connection)
    preview_silver_tables(snowflake_connection)
    check_silver_record_counts(snowflake_connection)

    snowflake_connection.close()