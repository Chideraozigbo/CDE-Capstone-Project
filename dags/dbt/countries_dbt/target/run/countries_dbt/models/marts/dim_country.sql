
  
    

        create or replace transient table country_database._mart_schema.dim_country
         as
        (

WITH country_base AS (
    SELECT 
        country_code,
        Country_Name,
        official_name,
        common_native_name,
        capital,
        region,
        subregion,
        area,
        continents,
        start_of_week
    FROM country_database._staging_schema.stg_country
)
SELECT 
    md5(cast(coalesce(cast(country_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as country_key,
    *,
    current_timestamp() as created_at,
    'N/A' as record_status
FROM country_base
        );
      
  