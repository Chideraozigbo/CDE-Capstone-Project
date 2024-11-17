
  
    

        create or replace transient table country_database._mart_schema.bridge_country_languages
         as
        (
WITH exploded_languages AS (
    -- Get country codes and exploded languages from the staging table
    SELECT 
        c.country_code,
        l.language
    FROM 
        country_database._staging_schema.stg_country c
    JOIN 
        country_database._staging_schema.stg_country_languages l
    ON c.country_code = l.country_code
),

mapped_ids AS (
    -- Map the exploded data to their respective surrogate keys
    SELECT
        d.country_id,
        dl.language_id
    FROM 
        exploded_languages el
    LEFT JOIN country_database._mart_schema.dim_country d 
    ON el.country_code = d.country_code
    LEFT JOIN country_database._mart_schema.dim_language dl 
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
        );
      
  