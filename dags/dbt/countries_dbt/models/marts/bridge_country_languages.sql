-- This model generates a table that creates a unique mapping of countries and their languages 
-- by creating surrogate keys for each combination of `country_code` and `language` 
-- from the `stg_country_languages` staging table.
-- The fields included are:
-- - `country_language_key`: a unique key for the combination of country and language,
-- - `country_key`: the surrogate key for the country,
-- - `language_key`: the surrogate key for the language,
-- - `country_code`: the unique code representing the country,
-- - `language`: the name of the language spoken in the country,
-- - `created_at`: timestamp indicating when the record was created.


{{ config(materialized='table') }}
SELECT
    {{ dbt_utils.generate_surrogate_key(['country_code', 'language']) }} as country_language_key,
    {{ dbt_utils.generate_surrogate_key(['country_code']) }} as country_key,
    {{ dbt_utils.generate_surrogate_key(['language']) }} as language_key,
    country_code,
    language,
    current_timestamp() as created_at
FROM {{ ref('stg_country_languages') }}