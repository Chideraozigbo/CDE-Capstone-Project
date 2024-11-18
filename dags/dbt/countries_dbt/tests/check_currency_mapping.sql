SELECT *
FROM {{ ref('fact_country_metric') }}
WHERE currency_id IS NULL
