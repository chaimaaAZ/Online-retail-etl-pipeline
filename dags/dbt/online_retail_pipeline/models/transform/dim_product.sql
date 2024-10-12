select distinct 
{{ dbt_utils.generate_surrogate_key(['STOCKCODE', 'DESCRIPTION', 'UNITPRICE'])}} as product_id,
   STOCKCODE as stock_code,
   DESCRIPTION as Description,
   UNITPRICE as price
from {{ source('online_retail_pipeline','raw_invoices')}}
where STOCKCODE is NOT NULL
and UNITPRICE > 0