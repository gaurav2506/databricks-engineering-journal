-- Create a Delta table to act as a materialized view
CREATE OR REPLACE TABLE materialized_sales_view
USING DELTA
AS
SELECT 
    region,
    SUM(sales) AS total_sales,
    COUNT(order_id) AS order_count
FROM sales_data
GROUP BY region;

-- Refresh logic
INSERT OVERWRITE TABLE materialized_sales_view
SELECT 
    region,
    SUM(sales) AS total_sales,
    COUNT(order_id) AS order_count
FROM sales_data
GROUP BY region;
