
  
    

        create or replace transient table country_database._mart_schema.dim_language
         as
        (
WITH language_base AS (
    SELECT DISTINCT
        language
    FROM country_database._staging_schema.stg_country_languages
    WHERE language IS NOT NULL
)
SELECT 
    md5(cast(coalesce(cast(language as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as language_id,
    language,
    current_timestamp() as created_at,
    'N/A' as record_status
FROM language_base
        );
      
  