
  create or replace   view country_database._staging_schema.stg_country
  
   as (
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
  );

