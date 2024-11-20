SELECT fc.country_id
FROM country_database._mart_schema.fact_country_metric fc
LEFT JOIN country_database._staging_schema.stg_country sc
ON fc.country_id = sc.country_id
WHERE sc.country_id IS NULL