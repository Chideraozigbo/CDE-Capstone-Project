SELECT fc.country_id
FROM {{ ref('fact_country_metric') }} fc
LEFT JOIN {{ ref('stg_country') }} sc
ON fc.country_id = sc.country_id
WHERE sc.country_id IS NULL
