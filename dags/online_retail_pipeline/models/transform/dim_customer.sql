with temp_customer as (
    select distinct 
    {{ dbt_utils.generate_surrogate_key(['CUSTOMERID','COUNTRY'])}} as customer_id,
    country as country 
    from {{source('online_retail_pipeline','raw_invoices')}}
    where CUSTOMERID IS NOT NULL
)

select t.* , cc.iso
from  temp_customer as t
left join {{source('online_retail_pipeline','country')}} as cc 
on t.COUNTRY = cc.nicename
