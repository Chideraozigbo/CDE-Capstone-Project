select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      SELECT *
FROM country_database._mart_schema.fact_country_metric
WHERE population < 0 OR area < 0
      
    ) dbt_internal_test