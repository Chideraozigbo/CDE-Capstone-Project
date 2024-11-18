select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select language
from country_database._mart_schema.dim_language
where language is null



      
    ) dbt_internal_test