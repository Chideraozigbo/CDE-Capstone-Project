
  
    

        create or replace transient table country_database._mart_schema.dim_country
         as
        (

WITH country_base AS (
    SELECT 
        country_id,
        country_code,
        Country_Name,
        official_name,
        common_native_name,
        independence,
        united_nation_members,
        capital,
        region,
        subregion,
        continents,
        start_of_week
    FROM country_database._staging_schema.stg_country
)
SELECT 
    *,
    current_timestamp() as created_at,
    'N/A' as record_status
FROM country_base
        );
      
  