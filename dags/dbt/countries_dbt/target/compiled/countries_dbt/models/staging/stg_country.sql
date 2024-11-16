-- This query extracts raw country information, including demographic, political, and geographical details, from the `country_data` source table.
-- The dataset includes fields such as country name, independence status, capital, region, population, languages, and currency information.


WITH raw_countries AS(
    SELECT
        Country_Name,
        independence,
        united_nation_members,
        start_of_week,
        official_name,
        common_native_name,
        currency_code,
        currency_name,
        currency_symbol,
        country_code,
        capital,
        region,
        subregion,
        languages,
        area,
        population,
        continents
    from country_database.raw_country_schema.country_data
)
SELECT
    Country_Name,
    independence,
    united_nation_members,
    start_of_week,
    official_name,
    common_native_name,
    currency_code,
    currency_name,
    currency_symbol,
    country_code,
    capital,
    region,
    subregion,
    languages,
    area,
    population,
    continents
FROM raw_countries