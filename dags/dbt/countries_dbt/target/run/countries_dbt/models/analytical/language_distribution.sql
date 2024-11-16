
  
    

        create or replace transient table country_database._analytical_schema.language_distribution
         as
        (WITH language_stats AS (
    SELECT 
        l.language,
        COUNT(DISTINCT bl.country_code) AS speaking_countries,
        SUM(f.population) AS total_speakers_potential,
        LISTAGG(DISTINCT c.region, ', ') WITHIN GROUP (ORDER BY c.region) AS regions_present
    FROM country_database._mart_schema.dim_language l
    JOIN country_database._mart_schema.bridge_country_languages bl 
        ON l.language_key = bl.language_key
    JOIN country_database._mart_schema.dim_country c 
        ON bl.country_key = c.country_key
    JOIN country_database._mart_schema.fct_country_stats f 
        ON c.country_key = f.country_key
    GROUP BY 1
)
SELECT 
    *,
    ROUND(speaking_countries * 100.0 / (SELECT COUNT(DISTINCT country_code) FROM country_database._staging_schema.stg_country), 2) AS pct_countries_speaking,
    CASE 
        WHEN speaking_countries = 1 THEN 'Unique to one country'
        WHEN speaking_countries <= 3 THEN 'Regional'
        WHEN speaking_countries <= 10 THEN 'Multi-regional'
        ELSE 'Widespread'
    END AS distribution_category
FROM language_stats
        );
      
  