SELECT 
    country_code,
    TRIM(language) AS language
FROM {{ ref('stg_country') }},
UNNEST(SPLIT(languages, ', ')) AS language