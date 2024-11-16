
    
    

select
    currency_code as unique_field,
    count(*) as n_records

from country_database._mart_schema.dim_currency
where currency_code is not null
group by currency_code
having count(*) > 1


