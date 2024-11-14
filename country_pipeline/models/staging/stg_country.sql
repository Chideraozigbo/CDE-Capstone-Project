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
    from {{ source('raw', 'country_data') }}
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