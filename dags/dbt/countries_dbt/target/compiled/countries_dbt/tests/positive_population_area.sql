SELECT *
FROM country_database._mart_schema.fact_country_metric
WHERE population < 0 OR area < 0