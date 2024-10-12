with temp_fact_invoices as (
    select 
     INVOICENO as invoice_id,
     INVOICEDATE as datetime_id,
     {{dbt_utils.generate_surrogate_key(['STOCKCODE', 'DESCRIPTION', 'UNITPRICE'])}} as product_id,
     {{ dbt_utils.generate_surrogate_key(['CUSTOMERID','COUNTRY'])}} as customer_id,
     QUANTITY as quantity,
     QUANTITY * UNITPRICE as total
     from {{ source('online_retail_pipeline', 'raw_invoices') }}
     where QUANTITY > 0

)

SELECT
    INVOICE_ID,
    dt.DATETIME_ID,
    dp.PRODUCT_ID,
    dc.CUSTOMER_ID,
    QUANTITY,
    TOTAL
FROM temp_fact_invoices fi
INNER JOIN {{ ref('dim_datetime') }} dt ON fi.DATETIME_ID = dt.DATETIME_ID
INNER JOIN {{ ref('dim_product') }} dp ON fi.PRODUCT_ID = dp.PRODUCT_ID
INNER JOIN {{ ref('dim_customer') }} dc ON fi.CUSTOMER_ID = dc.CUSTOMER_ID