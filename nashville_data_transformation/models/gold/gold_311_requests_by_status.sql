{{ config(materialized='table') }}

SELECT
    status,
    COUNT(*) AS total_requests

FROM {{ ref('silver_nashville_311_service_requests') }}

WHERE status IS NOT NULL
  AND TRIM(status) <> ''

GROUP BY status

ORDER BY total_requests DESC