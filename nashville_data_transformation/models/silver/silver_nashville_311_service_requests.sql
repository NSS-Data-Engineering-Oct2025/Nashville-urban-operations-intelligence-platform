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
FROM {{ source('raw_data', 'BRONZE_NASHVILLE_311_SERVICE_REQUESTS') }}