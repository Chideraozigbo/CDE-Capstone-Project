SELECT 
    country_id,
    currency_id,
    COUNT(*)
FROM {{ ref('fact_country_metric') }}
GROUP BY country_id, currency_id
HAVING COUNT(*) > 1
