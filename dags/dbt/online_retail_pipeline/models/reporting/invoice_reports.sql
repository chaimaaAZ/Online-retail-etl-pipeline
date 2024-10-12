select c.COUNTRY, 
c.ISO,
count(fc.INVOICE_ID) as total_invoices,
sum(fc.TOTAL) as revenue
from {{ref('fact_invoices')}} as fc
join {{ref('dim_customer')}} as c on fc.CUSTOMER_ID = c.CUSTOMER_ID
group by c.COUNTRY,c.ISO
order by revenue desc 