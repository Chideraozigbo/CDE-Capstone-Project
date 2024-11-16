
  
    

        create or replace transient table country_database._mart_schema.fct_country_stats
         as
        (
WITH base_stats AS (
    SELECT
        md5(cast(coalesce(cast(c.country_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as country_key,
        md5(cast(coalesce(cast(c.currency_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as currency_key,
        c.country_code,
        c.population,
        c.independence,
        c.united_nation_members,
        c.area,
        ROUND(c.population / NULLIF(c.area, 0), 2) as population_density,
        l.language_count,
        current_timestamp() as created_at
    FROM country_database._staging_schema.stg_country c
    LEFT JOIN (
        SELECT 
            country_code,
            COUNT(DISTINCT language) as language_count
        FROM country_database._staging_schema.stg_country_languages
        GROUP BY 1
    ) l ON c.country_code = l.country_code
)
SELECT 
    *,
    CASE 
        WHEN population_density < 50 THEN 'Low Density'
        WHEN population_density < 150 THEN 'Medium Density'
        ELSE 'High Density'
    END as density_category,
    CASE 
        WHEN language_count = 1 THEN 'Single Language'
        WHEN language_count = 2 THEN 'Bilingual'
        WHEN language_count > 2 THEN 'Multilingual'
        ELSE 'Unknown'
    END as language_diversity
FROM base_stats
        );
      
  