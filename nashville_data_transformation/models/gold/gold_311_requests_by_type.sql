{{ config(materialized='table') }}

SELECT
    request_type,
    COUNT(*) AS total_requests

FROM {{ ref('silver_nashville_311_service_requests') }}

WHERE request_type IS NOT NULL
  AND TRIM(request_type) <> ''

GROUP BY request_type

ORDER BY total_requests DESC