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

    create_311_silver_table_query = """
    CREATE OR REPLACE TABLE SILVER_NASHVILLE_311_SERVICE_REQUESTS AS
    SELECT
        RECORD_DATA:attributes.Request__::STRING AS REQUEST_ID,
        RECORD_DATA:attributes.Request_Type::STRING AS REQUEST_TYPE,
        RECORD_DATA:attributes.Subrequest_Type::STRING AS SUBREQUEST_TYPE,
        RECORD_DATA:attributes.Additional_Subrequest_Type::STRING AS ADDITIONAL_SUBREQUEST_TYPE,
        RECORD_DATA:attributes.Status::STRING AS STATUS,
        RECORD_DATA:attributes.Address::STRING AS ADDRESS,
        RECORD_DATA:attributes.City::STRING AS CITY,
        RECORD_DATA:attributes.ZIP::STRING AS ZIP_CODE,
        RECORD_DATA:attributes.Council_District::INTEGER AS COUNCIL_DISTRICT,
        TO_TIMESTAMP_NTZ(RECORD_DATA:attributes.Date_Time_Opened::NUMBER / 1000) AS DATE_TIME_OPENED,
        TO_TIMESTAMP_NTZ(RECORD_DATA:attributes.Date_Time_Closed::NUMBER / 1000) AS DATE_TIME_CLOSED,
        RECORD_DATA:attributes.Latitude::FLOAT AS LATITUDE,
        RECORD_DATA:attributes.Longitude::FLOAT AS LONGITUDE
    FROM BRONZE_NASHVILLE_311_SERVICE_REQUESTS;
    """

    create_housing_silver_table_query = """
    CREATE OR REPLACE TABLE SILVER_NASHVILLE_HOUSING_PROPERTY_DATA AS
    SELECT
        RECORD_DATA:attributes.APN::STRING AS APN,
        RECORD_DATA:attributes.ParID::STRING AS PARCEL_ID,
        RECORD_DATA:attributes.Owner::STRING AS OWNER_NAME,
        RECORD_DATA:attributes.PropAddr::STRING AS PROPERTY_ADDRESS,
        RECORD_DATA:attributes.PropCity::STRING AS PROPERTY_CITY,
        RECORD_DATA:attributes.PropState::STRING AS PROPERTY_STATE,
        RECORD_DATA:attributes.PropZip::STRING AS PROPERTY_ZIP_CODE,
        RECORD_DATA:attributes.LUCode::STRING AS LAND_USE_CODE,
        RECORD_DATA:attributes.LUDesc::STRING AS LAND_USE_DESCRIPTION,
        RECORD_DATA:attributes.Acres::FLOAT AS ACRES,
        RECORD_DATA:attributes.SalePrice::FLOAT AS SALE_PRICE,
        RECORD_DATA:attributes.LandAppr::FLOAT AS LAND_APPRAISED_VALUE,
        RECORD_DATA:attributes.ImprAppr::FLOAT AS IMPROVEMENT_APPRAISED_VALUE,
        RECORD_DATA:attributes.TotlAppr::FLOAT AS TOTAL_APPRAISED_VALUE,
        RECORD_DATA:attributes.TaxDist::STRING AS TAX_DISTRICT,
        RECORD_DATA:attributes.Council::STRING AS COUNCIL_DISTRICT,
        RECORD_DATA:attributes.Tract::STRING AS CENSUS_TRACT
    FROM BRONZE_NASHVILLE_HOUSING_PROPERTY_DATA;
    """

    create_public_safety_silver_table_query = """
    CREATE OR REPLACE TABLE SILVER_NASHVILLE_PUBLIC_SAFETY_DATA AS
    SELECT
        RECORD_DATA:attributes.FacilityName::STRING AS FACILITY_NAME,
        RECORD_DATA:attributes.Address::STRING AS ADDRESS,
        RECORD_DATA:attributes.City::STRING AS CITY,
        RECORD_DATA:attributes.State::STRING AS STATE,
        RECORD_DATA:attributes.ZipCode::STRING AS ZIP_CODE,
        RECORD_DATA:attributes.PhoneNumber::STRING AS PHONE_NUMBER,
        RECORD_DATA:attributes.CommanderName::STRING AS COMMANDER_NAME,
        RECORD_DATA:attributes.Website::STRING AS WEBSITE
    FROM BRONZE_NASHVILLE_PUBLIC_SAFETY_DATA;
    """

    snowflake_cursor.execute(create_311_silver_table_query)
    print("SILVER_NASHVILLE_311_SERVICE_REQUESTS table created")

    snowflake_cursor.execute(create_housing_silver_table_query)
    print("SILVER_NASHVILLE_HOUSING_PROPERTY_DATA table created")

    snowflake_cursor.execute(create_public_safety_silver_table_query)
    print("SILVER_NASHVILLE_PUBLIC_SAFETY_DATA table created")

    snowflake_cursor.close()


def preview_silver_tables(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    preview_queries = [
        "SELECT REQUEST_ID, REQUEST_TYPE, SUBREQUEST_TYPE, STATUS, CITY FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS LIMIT 5;",
        "SELECT APN, OWNER_NAME, PROPERTY_CITY, LAND_USE_DESCRIPTION, TOTAL_APPRAISED_VALUE FROM SILVER_NASHVILLE_HOUSING_PROPERTY_DATA LIMIT 5;",
        "SELECT FACILITY_NAME, ADDRESS, CITY, STATE FROM SILVER_NASHVILLE_PUBLIC_SAFETY_DATA LIMIT 5;"
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
        "SELECT 'SILVER_NASHVILLE_311_SERVICE_REQUESTS' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM SILVER_NASHVILLE_311_SERVICE_REQUESTS;",
        "SELECT 'SILVER_NASHVILLE_HOUSING_PROPERTY_DATA' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM SILVER_NASHVILLE_HOUSING_PROPERTY_DATA;",
        "SELECT 'SILVER_NASHVILLE_PUBLIC_SAFETY_DATA' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM SILVER_NASHVILLE_PUBLIC_SAFETY_DATA;"
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