select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select currency_key
from country_database._mart_schema.dim_currency
where currency_key is null



      
    ) dbt_internal_test