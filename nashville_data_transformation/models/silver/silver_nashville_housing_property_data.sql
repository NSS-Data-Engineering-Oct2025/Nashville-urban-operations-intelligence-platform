{{ config(materialized='table') }}

SELECT
    f.value:attributes:"APN"::STRING AS parcel_id,

    COALESCE(
        f.value:attributes:"Owner"::STRING,
        f.value:attributes:"OwnerName"::STRING,
        f.value:attributes:"OwnerName1"::STRING
    ) AS owner_name,

    COALESCE(
        f.value:attributes:"PropAddr"::STRING,
        f.value:attributes:"PropertyAddress"::STRING,
        f.value:attributes:"Address"::STRING,
        f.value:attributes:"SitusAddress"::STRING
    ) AS property_address,

    UPPER(TRIM(
        COALESCE(
            f.value:attributes:"PropCity"::STRING,
            f.value:attributes:"PropertyCity"::STRING,
            f.value:attributes:"SitusCity"::STRING,
            f.value:attributes:"City"::STRING
        )
    )) AS property_city,

    COALESCE(
        f.value:attributes:"PropState"::STRING,
        f.value:attributes:"PropertyState"::STRING,
        f.value:attributes:"State"::STRING
    ) AS property_state,

    COALESCE(
        f.value:attributes:"PropZip"::STRING,
        f.value:attributes:"PropertyZip"::STRING,
        f.value:attributes:"Zip"::STRING
    ) AS property_zip_code,

    COALESCE(
        f.value:attributes:"LUDesc"::STRING,
        f.value:attributes:"LandUse"::STRING,
        f.value:attributes:"LandUseDescription"::STRING,
        f.value:attributes:"LUCode"::STRING
    ) AS land_use_description,

    TRY_TO_DOUBLE(
        COALESCE(
            f.value:attributes:"TotlAppr"::STRING,
            f.value:attributes:"TotalAppr"::STRING,
            f.value:attributes:"TotalAppraisal"::STRING,
            f.value:attributes:"TotalAppraisedValue"::STRING
        )
    ) AS total_appraised_value,

    TRY_TO_DOUBLE(
        COALESCE(
            f.value:attributes:"LandAppr"::STRING,
            f.value:attributes:"LandAppraisal"::STRING,
            f.value:attributes:"LandAppraisedValue"::STRING
        )
    ) AS land_appraised_value,

    TRY_TO_DOUBLE(
        COALESCE(
            f.value:attributes:"ImprAppr"::STRING,
            f.value:attributes:"ImprovementAppr"::STRING,
            f.value:attributes:"ImprovementAppraisal"::STRING
        )
    ) AS improvement_appraised_value,

    TRY_TO_DOUBLE(
        COALESCE(
            f.value:attributes:"Acres"::STRING,
            f.value:attributes:"DeededAcreage"::STRING
        )
    ) AS acres,

    TRY_TO_DOUBLE(
        COALESCE(
            f.value:attributes:"SalePrice"::STRING,
            f.value:attributes:"SaleAmount"::STRING
        )
    ) AS sale_price,

    source_file_name,

    loaded_at

FROM {{ source('raw_data', 'raw_nashville_housing_property_data') }},
LATERAL FLATTEN(INPUT => raw_data:features) f

WHERE f.value:attributes IS NOT NULL