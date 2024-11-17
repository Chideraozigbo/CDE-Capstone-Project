

{{ config(materialized='table') }}

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
    FROM {{ ref('stg_country') }}
)
SELECT 
    *,
    current_timestamp() as created_at,
    'N/A' as record_status
FROM country_base