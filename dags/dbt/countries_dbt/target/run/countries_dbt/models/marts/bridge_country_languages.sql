
  
    

        create or replace transient table country_database._mart_schema.bridge_country_languages
         as
        (
SELECT
    md5(cast(coalesce(cast(country_code as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(language as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as country_language_key,
    md5(cast(coalesce(cast(country_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as country_key,
    md5(cast(coalesce(cast(language as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as language_key,
    country_code,
    language,
    current_timestamp() as created_at
FROM country_database._staging_schema.stg_country_languages
        );
      
  