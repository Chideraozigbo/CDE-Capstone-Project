-- This model creates a table of unique currencies and associated details (currency code, name, and symbol)
-- from the `stg_country` table. It generates a surrogate key (`currency_key`) for each currency using 
-- `dbt_utils.generate_surrogate_key`.
-- The fields include:
-- - `currency_code`: the unique identifier for the currency,
-- - `currency_name`: the name of the currency,
-- - `currency_symbol`: the symbol representing the currency,
-- - `created_at`: timestamp indicating when the record was created,
-- - `record_status`: a default field set to 'N/A' as a placeholder for potential future status tracking.
-- The model filters out any records where `currency_code` is NULL.


{{ config(materialized='table') }}
WITH currency_base AS (
    SELECT DISTINCT currency_code,
        currency_name,
        currency_symbol
    FROM {{ ref('stg_country') }}
    WHERE currency_code IS NOT NULL
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['currency_code']) }} as currency_key,
    *,
    current_timestamp() as created_at,
    'N/A' as record_status
FROM currency_base