-- This model aggregates language statistics by joining data from the `dim_language`, 
-- `bridge_country_languages`, `dim_country`, and `fct_country_stats` tables. It generates 
-- key language statistics including:
-- - `speaking_countries`: The number of distinct countries that speak a particular language.
-- - `total_speakers_potential`: The sum of populations of countries that speak the language.
-- - `regions_present`: A list of regions where the language is spoken, aggregated and ordered.
-- Additionally, it calculates:
-- - `pct_countries_speaking`: The percentage of countries that speak the language relative to 
--   the total number of distinct countries.
-- - `distribution_category`: A categorical label that describes the spread of the language:
--   - 'Unique to one country' for languages spoken in only one country.
--   - 'Regional' for languages spoken in up to three countries.
--   - 'Multi-regional' for languages spoken in up to ten countries.
--   - 'Widespread' for languages spoken in more than ten countries.


WITH language_stats AS (
    SELECT 
        l.language,
        COUNT(DISTINCT bl.country_code) AS speaking_countries,
        SUM(f.population) AS total_speakers_potential,
        LISTAGG(DISTINCT c.region, ', ') WITHIN GROUP (ORDER BY c.region) AS regions_present
    FROM {{ ref('dim_language') }} l
    JOIN {{ ref('bridge_country_languages') }} bl 
        ON l.language_key = bl.language_key
    JOIN {{ ref('dim_country') }} c 
        ON bl.country_key = c.country_key
    JOIN {{ ref('fct_country_stats') }} f 
        ON c.country_key = f.country_key
    GROUP BY 1
)
SELECT 
    *,
    ROUND(speaking_countries * 100.0 / (SELECT COUNT(DISTINCT country_code) FROM {{ ref('stg_country') }}), 2) AS pct_countries_speaking,
    CASE 
        WHEN speaking_countries = 1 THEN 'Unique to one country'
        WHEN speaking_countries <= 3 THEN 'Regional'
        WHEN speaking_countries <= 10 THEN 'Multi-regional'
        ELSE 'Widespread'
    END AS distribution_category
FROM language_stats
