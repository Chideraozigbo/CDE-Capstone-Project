
  create or replace   view country_database._staging_schema.stg_country_languages
  
   as (
    SELECT 
    country_code,
    TRIM(value) AS language
FROM country_database._staging_schema.stg_country,
LATERAL FLATTEN(input => SPLIT(languages, ', ')) AS language
  );

