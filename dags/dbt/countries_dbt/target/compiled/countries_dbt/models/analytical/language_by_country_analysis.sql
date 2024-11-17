

SELECT
    l.language,
    COUNT(DISTINCT f.country_id) AS country_count,
    SUM(f.population) AS total_population,
    AVG(f.population_density) AS avg_population_density
FROM country_database._mart_schema.bridge_country_languages b
JOIN country_database._mart_schema.dim_language l
    ON b.language_id = l.language_id
JOIN country_database._mart_schema.fact_country_metric f
    ON b.country_id = f.country_id
GROUP BY l.language