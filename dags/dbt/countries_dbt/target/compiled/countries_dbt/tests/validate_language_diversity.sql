SELECT *
FROM country_database._mart_schema.fact_country_metric
WHERE 
    (language_diversity_category = 'Monolingual' AND language_count != 1)
    OR (language_diversity_category = 'Moderately Multilingual' AND (language_count < 2 OR language_count > 4))
    OR (language_diversity_category = 'Highly Multilingual' AND language_count <= 4)