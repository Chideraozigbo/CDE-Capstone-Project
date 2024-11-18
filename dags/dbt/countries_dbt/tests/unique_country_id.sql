SELECT country_id, COUNT(*)
FROM {{ ref('fact_country_metric') }}
GROUP BY country_id
HAVING COUNT(*) > 1
