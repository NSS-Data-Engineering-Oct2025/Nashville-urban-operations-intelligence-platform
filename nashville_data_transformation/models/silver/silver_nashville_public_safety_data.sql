SELECT
    RECORD_DATA:attributes.FacilityName::STRING AS FACILITY_NAME,
    RECORD_DATA:attributes.Address::STRING AS ADDRESS,
    RECORD_DATA:attributes.City::STRING AS CITY,
    RECORD_DATA:attributes.State::STRING AS STATE,
    RECORD_DATA:attributes.ZipCode::STRING AS ZIP_CODE,
    RECORD_DATA:attributes.PhoneNumber::STRING AS PHONE_NUMBER,
    RECORD_DATA:attributes.CommanderName::STRING AS COMMANDER_NAME,
    RECORD_DATA:attributes.Website::STRING AS WEBSITE
FROM {{ source('raw_data', 'BRONZE_NASHVILLE_PUBLIC_SAFETY_DATA') }}