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


def create_snowflake_stage(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    create_stage_query = f"""
    CREATE OR REPLACE STAGE nashville_s3_stage
    URL='s3://{os.getenv("S3_BUCKET_NAME")}/{os.getenv("S3_BASE_FOLDER")}/raw_data/'
    CREDENTIALS = (
        AWS_KEY_ID = '{os.getenv("AWS_ACCESS_KEY_ID")}'
        AWS_SECRET_KEY = '{os.getenv("AWS_SECRET_ACCESS_KEY")}'
        AWS_TOKEN = '{os.getenv("AWS_SESSION_TOKEN")}'
    )
    FILE_FORMAT = (TYPE = JSON);
    """

    snowflake_cursor.execute(create_stage_query)
    print("Snowflake stage created successfully")

    snowflake_cursor.close()


def create_raw_tables(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    create_311_table_query = """
    CREATE OR REPLACE TABLE RAW_NASHVILLE_311_SERVICE_REQUESTS (
        RAW_DATA VARIANT,
        SOURCE_FILE_NAME STRING,
        LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """

    create_housing_table_query = """
    CREATE OR REPLACE TABLE RAW_NASHVILLE_HOUSING_PROPERTY_DATA (
        RAW_DATA VARIANT,
        SOURCE_FILE_NAME STRING,
        LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """

    create_public_safety_table_query = """
    CREATE OR REPLACE TABLE RAW_NASHVILLE_PUBLIC_SAFETY_DATA (
        RAW_DATA VARIANT,
        SOURCE_FILE_NAME STRING,
        LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );
    """

    snowflake_cursor.execute(create_311_table_query)
    print("RAW_NASHVILLE_311_SERVICE_REQUESTS table created successfully")

    snowflake_cursor.execute(create_housing_table_query)
    print("RAW_NASHVILLE_HOUSING_PROPERTY_DATA table created successfully")

    snowflake_cursor.execute(create_public_safety_table_query)
    print("RAW_NASHVILLE_PUBLIC_SAFETY_DATA table created successfully")

    snowflake_cursor.close()


def load_raw_data_from_stage(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    copy_311_data_query = """
    COPY INTO RAW_NASHVILLE_311_SERVICE_REQUESTS (RAW_DATA, SOURCE_FILE_NAME)
    FROM (
        SELECT
            $1,
            METADATA$FILENAME
        FROM @nashville_s3_stage/nashville_311_service_requests/
    )
    FILE_FORMAT = (TYPE = JSON);
    """

    copy_housing_data_query = """
    COPY INTO RAW_NASHVILLE_HOUSING_PROPERTY_DATA (RAW_DATA, SOURCE_FILE_NAME)
    FROM (
        SELECT
            $1,
            METADATA$FILENAME
        FROM @nashville_s3_stage/nashville_housing_property_data/
    )
    FILE_FORMAT = (TYPE = JSON);
    """

    copy_public_safety_data_query = """
    COPY INTO RAW_NASHVILLE_PUBLIC_SAFETY_DATA (RAW_DATA, SOURCE_FILE_NAME)
    FROM (
        SELECT
            $1,
            METADATA$FILENAME
        FROM @nashville_s3_stage/nashville_public_safety_data/
    )
    FILE_FORMAT = (TYPE = JSON);
    """

    snowflake_cursor.execute(copy_311_data_query)
    print("311 service request data loaded into Snowflake")

    snowflake_cursor.execute(copy_housing_data_query)
    print("Housing property data loaded into Snowflake")

    snowflake_cursor.execute(copy_public_safety_data_query)
    print("Public safety data loaded into Snowflake")

    snowflake_cursor.close()


def check_loaded_record_counts(snowflake_connection):
    snowflake_cursor = snowflake_connection.cursor()

    record_count_queries = [
        "SELECT 'RAW_NASHVILLE_311_SERVICE_REQUESTS' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM RAW_NASHVILLE_311_SERVICE_REQUESTS;",
        "SELECT 'RAW_NASHVILLE_HOUSING_PROPERTY_DATA' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM RAW_NASHVILLE_HOUSING_PROPERTY_DATA;",
        "SELECT 'RAW_NASHVILLE_PUBLIC_SAFETY_DATA' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM RAW_NASHVILLE_PUBLIC_SAFETY_DATA;"
    ]

    print("\nLoaded record counts:")

    for record_count_query in record_count_queries:
        snowflake_cursor.execute(record_count_query)
        record_count_result = snowflake_cursor.fetchone()
        print(record_count_result)

    snowflake_cursor.close()


if __name__ == "__main__":
    snowflake_connection = create_snowflake_connection()

    create_snowflake_stage(snowflake_connection)
    create_raw_tables(snowflake_connection)
    load_raw_data_from_stage(snowflake_connection)
    check_loaded_record_counts(snowflake_connection)

    snowflake_connection.close()