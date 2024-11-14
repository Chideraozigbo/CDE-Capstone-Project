-- This model creates a table that combines various country statistics from `stg_country` and `stg_country_languages`.
-- It includes fields such as population, area, independence status, and United Nations membership,
-- and calculates additional metrics like population density and language diversity.
-- Surrogate keys for `country_key` and `currency_key` are generated using the `dbt_utils.generate_surrogate_key` macro.
-- The table categorizes countries by population density (Low, Medium, or High Density) and language diversity (Single Language, Bilingual, Multilingual, or Unknown).
-- Timestamp of record creation is stored in `created_at`.


{{ config(materialized='table') }}
WITH base_stats AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['c.country_code']) }} as country_key,
        {{ dbt_utils.generate_surrogate_key(['c.currency_code']) }} as currency_key,
        c.country_code,
        c.population,
        c.independence,
        c.united_nation_members,
        c.area,
        ROUND(c.population / NULLIF(c.area, 0), 2) as population_density,
        l.language_count,
        current_timestamp() as created_at
    FROM {{ ref('stg_country') }} c
    LEFT JOIN (
        SELECT 
            country_code,
            COUNT(DISTINCT language) as language_count
        FROM {{ ref('stg_country_languages') }}
        GROUP BY 1
    ) l ON c.country_code = l.country_code
)
SELECT 
    *,
    CASE 
        WHEN population_density < 50 THEN 'Low Density'
        WHEN population_density < 150 THEN 'Medium Density'
        ELSE 'High Density'
    END as density_category,
    CASE 
        WHEN language_count = 1 THEN 'Single Language'
        WHEN language_count = 2 THEN 'Bilingual'
        WHEN language_count > 2 THEN 'Multilingual'
        ELSE 'Unknown'
    END as language_diversity
FROM base_stats