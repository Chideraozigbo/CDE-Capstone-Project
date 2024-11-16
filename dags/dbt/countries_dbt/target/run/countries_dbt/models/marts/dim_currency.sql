
  
    

        create or replace transient table country_database._mart_schema.dim_currency
         as
        (
WITH currency_base AS (
    SELECT DISTINCT
        currency_code,
        currency_name,
        currency_symbol
    FROM country_database._staging_schema.stg_country
    WHERE currency_code IS NOT NULL
)
SELECT 
    md5(cast(coalesce(cast(currency_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as currency_key,
    *,
    current_timestamp() as created_at,
    'N/A' as record_status
FROM currency_base
        );
      
  