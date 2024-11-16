
  
    

        create or replace transient table country_database._analytical_schema.country_analysis
         as
        (WITH country_detail AS (
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
        ARRAY_AGG(DISTINCT l.language) as spoken_languages
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
      
  