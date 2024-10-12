select 
d.YEAR,
d.MONTH,
COUNT(DISTINCT fc.INVOICE_ID) AS total_invoices,
SUM(fc.TOTAL) AS total_revenue
FROM {{ ref('fact_invoices') }} fc
JOIN {{ ref('dim_datetime') }} d ON fc.DATETIME_ID = d.DATETIME_ID
GROUP BY d.YEAR, d.MONTH
ORDER BY d.YEAR,d.MONTH