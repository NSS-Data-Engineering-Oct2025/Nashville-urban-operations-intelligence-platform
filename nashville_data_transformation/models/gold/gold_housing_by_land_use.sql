{{ config(materialized='table') }}

SELECT
    land_use_description,
    COUNT(*) AS total_properties,
    AVG(total_appraised_value) AS average_appraised_value,
    MIN(total_appraised_value) AS minimum_appraised_value,
    MAX(total_appraised_value) AS maximum_appraised_value

FROM {{ ref('silver_nashville_housing_property_data') }}

WHERE land_use_description IS NOT NULL
  AND TRIM(land_use_description) <> ''

GROUP BY land_use_description

ORDER BY total_properties DESC