select 
p.PRODUCT_ID,
p.STOCK_CODE,
p.DESCRIPTION,
SUM(fc.QUANTITY) AS quantity_sold_per_product,
FROM {{ ref('fact_invoices') }} fc
JOIN {{ ref('dim_product') }} p ON fc.PRODUCT_ID = p.PRODUCT_ID
GROUP BY p.PRODUCT_ID, p.STOCK_CODE, p.DESCRIPTION
ORDER BY quantity_sold_per_product DESC