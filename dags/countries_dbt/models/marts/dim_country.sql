-- This model creates a table of unique countries and associated details (country code, name, capital, region, etc.) 
-- from the `stg_country` table. It generates a surrogate key (`country_key`) for each country using 
-- `dbt_utils.generate_surrogate_key`.
-- The fields include:
-- - `country_code`: the unique identifier for the country,
-- - `Country_Name`: the name of the country,
-- - `official_name`: the official name of the country,
-- - `common_native_name`: the native name of the country,
-- - `capital`: the capital city of the country,
-- - `region`: the broader geographic region of the country,
-- - `subregion`: a more specific subregion within the broader region,
-- - `area`: the total land area of the country,
-- - `continents`: the continents the country is located in,
-- - `start_of_week`: the country’s designated first day of the week,
-- - `created_at`: timestamp indicating when the record was created,
-- - `record_status`: a default field set to 'N/A' as a placeholder for potential future status tracking.


{{ config(materialized='table') }}

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
    FROM {{ ref('stg_country') }}
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['country_code']) }} as country_key,
    *,
    current_timestamp() as created_at,
    'N/A' as record_status
FROM country_base