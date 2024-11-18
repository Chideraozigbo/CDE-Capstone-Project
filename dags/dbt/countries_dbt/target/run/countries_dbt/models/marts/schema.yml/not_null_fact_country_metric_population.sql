select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select population
from country_database._mart_schema.fact_country_metric
where population is null



      
    ) dbt_internal_test