

SELECT
    d.region,
    d.subregion,
    COUNT(*) AS country_count,
    SUM(f.population) AS total_population,
    AVG(f.population_density) AS avg_population_density,
    MAX(f.population_density) AS max_population_density,
    AVG(f.language_count) AS avg_language_count,
    MAX(f.language_count) AS max_language_count
FROM country_database._mart_schema.fact_country_metric f
JOIN country_database._mart_schema.dim_country d
    ON f.country_id = d.country_id
GROUP BY d.region, d.subregion