{{ config(materialized="view") }}

SELECT
    l.language,
    COUNT(DISTINCT f.country_id) AS country_count,
    SUM(f.population) AS total_population,
    AVG(f.population_density) AS avg_population_density
FROM {{ ref('bridge_country_languages') }} b
JOIN {{ ref('dim_language') }} l
    ON b.language_id = l.language_id
JOIN {{ ref('fact_country_metric') }} f
    ON b.country_id = f.country_id
GROUP BY l.language
