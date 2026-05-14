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
        COALESCE(
            f.value:attributes:"GlobalID"::STRING,
            f.value:attributes:"OBJECTID"::STRING
        ) AS REQUEST_ID,

        COALESCE(
            f.value:attributes:"Request_Type"::STRING,
            f.value:attributes:"Additional_Subrequest_Type"::STRING,
            f.value:attributes:"Subrequest_Type"::STRING
        ) AS REQUEST_TYPE,

        COALESCE(
            f.value:attributes:"Subrequest_Type"::STRING,
            f.value:attributes:"Additional_Subrequest_Type"::STRING
        ) AS SUBREQUEST_TYPE,

        COALESCE(
            f.value:attributes:"Status"::STRING,
            'Closed'
        ) AS STATUS,

        UPPER(TRIM(f.value:attributes:"City"::STRING)) AS CITY,

        f.value:attributes:"Address"::STRING AS ADDRESS,

        f.value:attributes:"Council_District"::STRING AS COUNCIL_DISTRICT,

        TRY_TO_DOUBLE(f.value:attributes:"Latitude"::STRING) AS LATITUDE,

        TRY_TO_DOUBLE(f.value:attributes:"Longitude"::STRING) AS LONGITUDE,

        CASE
            WHEN TRY_TO_NUMBER(f.value:attributes:"Date_Time_Opened"::STRING) IS NOT NULL
            THEN TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(f.value:attributes:"Date_Time_Opened"::STRING) / 1000)
            ELSE TRY_TO_TIMESTAMP_NTZ(f.value:attributes:"Date_Time_Opened"::STRING)
        END AS DATE_TIME_OPENED,

        CASE
            WHEN TRY_TO_NUMBER(f.value:attributes:"Date_Time_Closed"::STRING) IS NOT NULL
            THEN TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(f.value:attributes:"Date_Time_Closed"::STRING) / 1000)
            ELSE TRY_TO_TIMESTAMP_NTZ(f.value:attributes:"Date_Time_Closed"::STRING)
        END AS DATE_TIME_CLOSED,

        SOURCE_FILE_NAME,
        LOADED_AT

    FROM RAW_NASHVILLE_311_SERVICE_REQUESTS,
    LATERAL FLATTEN(INPUT => RAW_DATA:features) f
    WHERE f.value:attributes IS NOT NULL;
    """

    create_housing_silver_query = """
    CREATE OR REPLACE TABLE SILVER_NASHVILLE_HOUSING_PROPERTY_DATA AS
    SELECT
        f.value:attributes:"APN"::STRING AS PARCEL_ID,

        COALESCE(
            f.value:attributes:"Owner"::STRING,
            f.value:attributes:"OwnerName"::STRING,
            f.value:attributes:"OwnerName1"::STRING
        ) AS OWNER_NAME,

        COALESCE(
            f.value:attributes:"PropAddr"::STRING,
            f.value:attributes:"PropertyAddress"::STRING,
            f.value:attributes:"Address"::STRING,
            f.value:attributes:"SitusAddress"::STRING
        ) AS PROPERTY_ADDRESS,

        UPPER(TRIM(
            COALESCE(
                f.value:attributes:"PropCity"::STRING,
                f.value:attributes:"PropertyCity"::STRING,
                f.value:attributes:"SitusCity"::STRING,
                f.value:attributes:"City"::STRING
            )
        )) AS PROPERTY_CITY,

        COALESCE(
            f.value:attributes:"PropState"::STRING,
            f.value:attributes:"PropertyState"::STRING,
            f.value:attributes:"State"::STRING
        ) AS PROPERTY_STATE,

        COALESCE(
            f.value:attributes:"PropZip"::STRING,
            f.value:attributes:"PropertyZip"::STRING,
            f.value:attributes:"Zip"::STRING
        ) AS PROPERTY_ZIP_CODE,

        COALESCE(
            f.value:attributes:"LUDesc"::STRING,
            f.value:attributes:"LandUse"::STRING,
            f.value:attributes:"LandUseDescription"::STRING,
            f.value:attributes:"LUCode"::STRING
        ) AS LAND_USE_DESCRIPTION,

        TRY_TO_DOUBLE(
            COALESCE(
                f.value:attributes:"TotlAppr"::STRING,
                f.value:attributes:"TotalAppr"::STRING,
                f.value:attributes:"TotalAppraisal"::STRING,
                f.value:attributes:"TotalAppraisedValue"::STRING
            )
        ) AS TOTAL_APPRAISED_VALUE,

        TRY_TO_DOUBLE(
            COALESCE(
                f.value:attributes:"LandAppr"::STRING,
                f.value:attributes:"LandAppraisal"::STRING,
                f.value:attributes:"LandAppraisedValue"::STRING
            )
        ) AS LAND_APPRAISED_VALUE,

        TRY_TO_DOUBLE(
            COALESCE(
                f.value:attributes:"ImprAppr"::STRING,
                f.value:attributes:"ImprovementAppr"::STRING,
                f.value:attributes:"ImprovementAppraisal"::STRING
            )
        ) AS IMPROVEMENT_APPRAISED_VALUE,

        TRY_TO_DOUBLE(
            COALESCE(
                f.value:attributes:"Acres"::STRING,
                f.value:attributes:"DeededAcreage"::STRING
            )
        ) AS ACRES,

        TRY_TO_DOUBLE(
            COALESCE(
                f.value:attributes:"SalePrice"::STRING,
                f.value:attributes:"SaleAmount"::STRING
            )
        ) AS SALE_PRICE,

        SOURCE_FILE_NAME,
        LOADED_AT

    FROM RAW_NASHVILLE_HOUSING_PROPERTY_DATA,
    LATERAL FLATTEN(INPUT => RAW_DATA:features) f
    WHERE f.value:attributes IS NOT NULL;
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
        LIMIT 10;
        """,
        """
        SELECT
            PARCEL_ID,
            PROPERTY_CITY,
            LAND_USE_DESCRIPTION,
            TOTAL_APPRAISED_VALUE
        FROM SILVER_NASHVILLE_HOUSING_PROPERTY_DATA
        LIMIT 10;
        """
    ]

    print("\\nSilver table previews:")

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

    print("\\nSilver table record counts:")

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