

{{ config(materialized='table') }}
WITH language_base AS (
    SELECT DISTINCT
        language
    FROM {{ ref('stg_country_languages') }}
    WHERE language IS NOT NULL
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['language']) }} as language_id,
    language,
    current_timestamp() as created_at,
    'N/A' as record_status
FROM language_base