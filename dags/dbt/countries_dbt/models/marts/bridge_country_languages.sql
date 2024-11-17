


{{ config(materialized='table') }}
WITH exploded_languages AS (
    -- Get country codes and exploded languages from the staging table
    SELECT 
        c.country_code,
        l.language
    FROM 
        {{ ref('stg_country') }} c
    JOIN 
        {{ ref('stg_country_languages') }} l
    ON c.country_code = l.country_code
),

mapped_ids AS (
    -- Map the exploded data to their respective surrogate keys
    SELECT
        d.country_id,
        dl.language_id
    FROM 
        exploded_languages el
    LEFT JOIN {{ ref('dim_country') }} d 
    ON el.country_code = d.country_code
    LEFT JOIN {{ ref('dim_language') }} dl 
    ON el.language = dl.language
),

deduplicated_bridge AS (
    -- Ensure no duplicates in the bridge table
    SELECT DISTINCT
        country_id,
        language_id
    FROM mapped_ids
)

-- Final output
SELECT 
    country_id,
    language_id
FROM 
    deduplicated_bridge
