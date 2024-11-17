{{ config(materialized="view") }}

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
FROM {{ ref('fact_country_metric') }} f
JOIN {{ ref('dim_country') }} d
    ON f.country_id = d.country_id
LEFT JOIN {{ ref('dim_currency') }} c
    ON f.currency_id = c.currency_id
