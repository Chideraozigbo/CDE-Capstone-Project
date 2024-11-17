WITH country_base AS (
    SELECT 
        DISTINCT
        c.country_id,
        c.country_name,
        CAST(c.population AS FLOAT) AS population,
        CAST(c.area AS FLOAT) AS area,
        c.currency_code,
        c.region,
        c.subregion
    FROM {{ ref('stg_country') }} c
    WHERE c.population IS NOT NULL AND c.area IS NOT NULL
),

population_density AS (
    SELECT 
        cb.country_id,
        cb.population / NULLIF(cb.area, 0) AS population_density 
    FROM country_base cb
),

density_category AS (
    SELECT 
        pd.country_id,
        pd.population_density,
        CASE 
            WHEN pd.population_density < 50 THEN 'Low Density'
            WHEN pd.population_density BETWEEN 50 AND 300 THEN 'Medium Density'
            ELSE 'High Density'
        END AS density_category
    FROM population_density pd
),

language_diversity AS (
    SELECT 
        bcl.country_id,
        COUNT(DISTINCT bcl.language_id) AS language_count
    FROM {{ ref('bridge_country_languages') }} bcl
    GROUP BY bcl.country_id
),

currency_mapping AS (
    SELECT DISTINCT
        cb.country_id,
        dc.currency_id
    FROM country_base cb
    LEFT JOIN {{ ref('dim_currency') }} dc
    ON cb.currency_code = dc.currency_code
),

fact_metrics AS (
    SELECT DISTINCT
        cb.country_id,
        cb.population,
        cb.area,
        cm.currency_id,
        dc.population_density,
        dc.density_category,
        ld.language_count,
        CASE 
            WHEN ld.language_count = 1 THEN 'Monolingual'
            WHEN ld.language_count BETWEEN 2 AND 4 THEN 'Moderately Multilingual'
            ELSE 'Highly Multilingual'
        END AS language_diversity_category
    FROM country_base cb
    LEFT JOIN density_category dc ON cb.country_id = dc.country_id
    LEFT JOIN language_diversity ld ON cb.country_id = ld.country_id
    LEFT JOIN currency_mapping cm ON cb.country_id = cm.country_id
)

SELECT 
    DISTINCT *, 
    current_timestamp() AS created_at,
    'N/A' AS record_status
FROM fact_metrics
