

SELECT 
    d.country_name,
    f.population,
    f.area,
    f.population_density,
    f.density_category,
    f.language_count,
    f.language_diversity_category,
    d.region,
    d.subregion,
    c.currency_name,
    c.currency_symbol
FROM country_database._mart_schema.fact_country_metric f
JOIN country_database._mart_schema.dim_country d
    ON f.country_id = d.country_id
LEFT JOIN country_database._mart_schema.dim_currency c
    ON f.currency_id = c.currency_id