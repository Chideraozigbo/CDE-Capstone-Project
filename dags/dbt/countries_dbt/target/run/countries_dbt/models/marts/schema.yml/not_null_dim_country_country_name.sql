select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select country_name
from country_database._mart_schema.dim_country
where country_name is null



      
    ) dbt_internal_test