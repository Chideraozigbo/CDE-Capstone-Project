select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      SELECT 
    country_id,
    currency_id,
    COUNT(*)
FROM country_database._mart_schema.fact_country_metric
GROUP BY country_id, currency_id
HAVING COUNT(*) > 1
      
    ) dbt_internal_test