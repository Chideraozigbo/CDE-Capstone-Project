
  
    

        create or replace transient table country_database._analytical_schema.country_analysis
         as
        (-- This model aggregates and enriches country-level details by joining data from 
-- the `dim_country`, `fct_country_stats`, `dim_currency`, `bridge_country_languages`, 
-- and `dim_language` tables. It generates comprehensive country details, including:
-- - Basic information: country name, region, subregion, capital, population, area.
-- - Country statistics: population density, density category, independence status,
--   United Nations membership, language count, and language diversity.
-- - Currency details: currency name and symbol.
-- - Languages spoken in the country, aggregated as an array of distinct languages.
-- Additionally, it computes:
-- - `avg_population_by_region`: Average population per region.
-- - `avg_density_by_continent`: Average population density per continent.
-- - `avg_languages_by_region`: Average number of languages spoken per region.
-- - `population_rank_in_region`: The rank of countries based on population within their region.
-- - `area_rank_in_continent`: The rank of countries based on area within their continent.


WITH country_detail AS (
    SELECT 
        c.country_key,
        c.country_name,
        c.region,
        c.subregion,
        c.continents,
        c.capital,
        f.population,
        f.area,
        f.population_density,
        f.density_category,
        f.independence,
        f.united_nation_members,
        f.language_count,
        f.language_diversity,
        cur.currency_name,
        cur.currency_symbol,
        l.language
    FROM country_database._mart_schema.dim_country c
    LEFT JOIN country_database._mart_schema.fct_country_stats f 
        ON c.country_key = f.country_key
    LEFT JOIN country_database._mart_schema.dim_currency cur 
        ON f.currency_key = cur.currency_key
    LEFT JOIN country_database._mart_schema.bridge_country_languages bl 
        ON c.country_key = bl.country_key
    LEFT JOIN country_database._mart_schema.dim_language l 
        ON bl.language_key = l.language_key
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)
SELECT 
    *,
    AVG(population) OVER (PARTITION BY region) as avg_population_by_region,
    AVG(population_density) OVER (PARTITION BY continents) as avg_density_by_continent,
    AVG(language_count) OVER (PARTITION BY region) as avg_languages_by_region,
    RANK() OVER (PARTITION BY region ORDER BY population DESC) as population_rank_in_region,
    RANK() OVER (PARTITION BY continents ORDER BY area DESC) as area_rank_in_continent
FROM country_detail
        );
      
  