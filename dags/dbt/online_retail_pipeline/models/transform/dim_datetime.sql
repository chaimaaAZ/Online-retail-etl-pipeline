-- Create a CTE to extract date and time components
WITH temp_datetime AS (  
  SELECT DISTINCT
    InvoiceDate AS datetime_id,
    CASE
      WHEN LENGTH(INVOICEDATE) = 16 THEN
        -- Date format: "DD/MM/YYYY HH:MM"
        TO_TIMESTAMP(INVOICEDATE, 'DD/MM/YYYY HH24:MI')
      WHEN LENGTH(INVOICEDATE) <= 14 THEN
        -- Date format: "MM/DD/YY HH:MM"
        TO_TIMESTAMP(INVOICEDATE, 'MM/DD/YY HH24:MI')
      ELSE
        NULL
    END AS date_part
  FROM {{ source('online_retail_pipeline', 'raw_invoices') }}
  WHERE INVOICEDATE IS NOT NULL
)
SELECT
  datetime_id,
  date_part as datetime,
  EXTRACT(YEAR FROM date_part) AS year,
  EXTRACT(MONTH FROM date_part) AS month,
  EXTRACT(DAY FROM date_part) AS day,
  EXTRACT(HOUR FROM date_part) AS hour,
  EXTRACT(MINUTE FROM date_part) AS minute,
  EXTRACT(DAYOFWEEK FROM date_part) AS weekday
FROM temp_datetime
