SELECT *
FROM country_database._mart_schema.fact_country_metric
WHERE 
    (density_category = 'Low Density' AND population_density >= 50)
    OR (density_category = 'Medium Density' AND (population_density < 50 OR population_density > 300))
    OR (density_category = 'High Density' AND population_density <= 300)