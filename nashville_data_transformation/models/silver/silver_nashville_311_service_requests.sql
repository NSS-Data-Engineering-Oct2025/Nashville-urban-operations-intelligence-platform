{{ config(materialized='table') }}

SELECT
    COALESCE(
        f.value:attributes:"GlobalID"::STRING,
        f.value:attributes:"OBJECTID"::STRING
    ) AS request_id,

    COALESCE(
        f.value:attributes:"Request_Type"::STRING,
        f.value:attributes:"Additional_Subrequest_Type"::STRING,
        f.value:attributes:"Subrequest_Type"::STRING
    ) AS request_type,

    COALESCE(
        f.value:attributes:"Subrequest_Type"::STRING,
        f.value:attributes:"Additional_Subrequest_Type"::STRING
    ) AS subrequest_type,

    COALESCE(
        f.value:attributes:"Status"::STRING,
        'Closed'
    ) AS status,

    UPPER(TRIM(f.value:attributes:"City"::STRING)) AS city,

    f.value:attributes:"Address"::STRING AS address,

    f.value:attributes:"Council_District"::STRING AS council_district,

    TRY_TO_DOUBLE(f.value:attributes:"Latitude"::STRING) AS latitude,

    TRY_TO_DOUBLE(f.value:attributes:"Longitude"::STRING) AS longitude,

    CASE
        WHEN TRY_TO_NUMBER(f.value:attributes:"Date_Time_Opened"::STRING) IS NOT NULL
        THEN TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(f.value:attributes:"Date_Time_Opened"::STRING) / 1000)
        ELSE TRY_TO_TIMESTAMP_NTZ(f.value:attributes:"Date_Time_Opened"::STRING)
    END AS date_time_opened,

    CASE
        WHEN TRY_TO_NUMBER(f.value:attributes:"Date_Time_Closed"::STRING) IS NOT NULL
        THEN TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(f.value:attributes:"Date_Time_Closed"::STRING) / 1000)
        ELSE TRY_TO_TIMESTAMP_NTZ(f.value:attributes:"Date_Time_Closed"::STRING)
    END AS date_time_closed,

    source_file_name,

    loaded_at

FROM {{ source('raw_data', 'raw_nashville_311_service_requests') }},
LATERAL FLATTEN(INPUT => raw_data:features) f

WHERE f.value:attributes IS NOT NULL