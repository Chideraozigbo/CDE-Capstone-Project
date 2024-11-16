-- This query extracts raw country information, including demographic, political, and geographical details, from the `country_data` source table.
-- The dataset includes fields such as country name, independence status, capital, region, population, languages, and currency information.


WITH raw_countries AS(
    SELECT
        TRIM(Country_Name) AS country_name,
        TRIM(independence) AS independence,
        TRIM(united_nation_members) AS union_nation_members,
        TRIM(start_of_week) AS start_of_week,
        TRIM(official_name) AS official_name,
        TRIM(common_native_name) AS common_native_name,
        TRIM(currency_code) AS currency_code,
        TRIM(currency_name) AS currency_name,
        TRIM(currency_symbol) AS currency_symbol,
        TRIM(country_code) AS country_code,
        TRIM(capital) AS capital,
        TRIM(region) AS region,
        TRIM(subregion) AS subregion,
        TRIM(area) AS area,
        TRIIM(population) AS population,
        TRIM(continents) AS continents
    from {{ source('raw', 'country_data') }}
)
SELECT
    *
FROM raw_countries