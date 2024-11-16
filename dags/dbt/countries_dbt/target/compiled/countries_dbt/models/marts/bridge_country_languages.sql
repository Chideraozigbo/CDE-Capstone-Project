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



SELECT
    md5(cast(coalesce(cast(country_code as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(language as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as country_language_key,
    md5(cast(coalesce(cast(country_code as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as country_key,
    md5(cast(coalesce(cast(language as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as language_key,
    country_code,
    language,
    current_timestamp() as created_at
FROM country_database._staging_schema.stg_country_languages