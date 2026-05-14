{{ config(materialized='table') }}

SELECT
    property_city,
    COUNT(*) AS total_properties,
    AVG(total_appraised_value) AS average_appraised_value

FROM {{ ref('silver_nashville_housing_property_data') }}

WHERE property_city IS NOT NULL
  AND TRIM(property_city) <> ''

GROUP BY property_city

ORDER BY total_properties DESC