
    
    

select
    currency_id as unique_field,
    count(*) as n_records

from country_database._mart_schema.dim_currency
where currency_id is not null
group by currency_id
having count(*) > 1


