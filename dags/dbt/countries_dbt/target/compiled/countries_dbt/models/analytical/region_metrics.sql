-- This model aggregates country statistics by region and continent. It computes various key metrics 
-- for each combination of region and continent, including:
-- - `number_of_countries`: The number of countries in the region/continent.
-- - `total_population`: The total population of countries in the region/continent.
-- - `avg_population`: The average population of countries in the region/continent.
-- - `total_area`: The total land area of countries in the region/continent.
-- - `avg_area`: The average area of countries in the region/continent.
-- - `avg_population_density`: The average population density across countries in the region/continent.
-- - `independent_countries`: The count of independent countries in the region/continent.
-- - `un_member_countries`: The count of countries that are members of the United Nations.
-- - `avg_languages_per_country`: The average number of languages spoken per country in the region/continent.
-- - `unique_currencies`: The count of unique currencies used in the region/continent.
-- Additionally, it calculates distribution metrics for:
-- - Country density (low, medium, high).
-- - Language diversity (monolingual, bilingual, multilingual).


SELECT 
    region,
    continents,
    COUNT(DISTINCT country_name) as number_of_countries,
    SUM(population) as total_population,
    ROUND(AVG(population), 0) as avg_population,
    SUM(area) as total_area,
    ROUND(AVG(area), 2) as avg_area,
    ROUND(AVG(population_density), 2) as avg_population_density,
    COUNT(CASE WHEN independence THEN 1 END) as independent_countries,
    COUNT(CASE WHEN united_nation_members THEN 1 END) as un_member_countries,
    ROUND(AVG(language_count), 2) as avg_languages_per_country,
    COUNT(DISTINCT currency_name) as unique_currencies,
    -- Density distribution
    COUNT(CASE WHEN density_category = 'Low Density' THEN 1 END) as low_density_countries,
    COUNT(CASE WHEN density_category = 'Medium Density' THEN 1 END) as medium_density_countries,
    COUNT(CASE WHEN density_category = 'High Density' THEN 1 END) as high_density_countries,
    -- Language diversity
    COUNT(CASE WHEN language_diversity = 'Single Language' THEN 1 END) as monolingual_countries,
    COUNT(CASE WHEN language_diversity = 'Bilingual' THEN 1 END) as bilingual_countries,
    COUNT(CASE WHEN language_diversity = 'Multilingual' THEN 1 END) as multilingual_countries
FROM country_database._analytical_schema.country_analysis
GROUP BY 1, 2