-- This query retrieves a list of languages spoken in each country by flattening 
-- the comma-separated `languages` field from `raw country_data` table. 
-- It uses Snowflake’s `LATERAL FLATTEN` function on the array of languages to create a 
-- row for each language associated with a given `country_code`.

SELECT 
    country_code,
    TRIM(value) AS language
FROM country_database.raw_country_schema.country_data,
LATERAL FLATTEN(input => SPLIT(languages, ', ')) AS language