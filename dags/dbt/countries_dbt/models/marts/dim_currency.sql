


{{ config(materialized='table') }}
WITH currency_base AS (
    SELECT DISTINCT currency_code,
        currency_name,
        currency_symbol
    FROM {{ ref('stg_country') }}
    WHERE currency_code IS NOT NULL
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['currency_code']) }} as currency_id,
    *,
    current_timestamp() as created_at,
    'N/A' as record_status
FROM currency_base