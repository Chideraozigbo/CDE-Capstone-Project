-- This model creates a table with unique languages and associated country codes from `stg_country_languages`.
-- It generates a surrogate key (`language_key`) for each language using `dbt_utils.generate_surrogate_key`.
-- Fields include:
-- - `language`: the name of the language,
-- - `created_at`: timestamp indicating when the record was created,
-- - `record_status`: a default field set to 'N/A' as a placeholder for potential future status tracking.
-- The model filters out any records where `language` is NULL.



WITH language_base AS (
    SELECT DISTINCT
        language,
        country_code
    FROM country_database._staging_schema.stg_country_languages
    WHERE language IS NOT NULL
)
SELECT 
    md5(cast(coalesce(cast(language as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as language_key,
    language,
    current_timestamp() as created_at,
    'N/A' as record_status
FROM language_base