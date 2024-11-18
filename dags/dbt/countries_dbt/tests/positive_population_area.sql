SELECT *
FROM {{ ref('fact_country_metric') }}
WHERE population < 0 OR area < 0
