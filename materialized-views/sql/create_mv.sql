-- Materialized view for revenue and order metrics by zone
CREATE OR REPLACE TABLE zone_revenue_mv
USING DELTA
AS
SELECT 
    zone,
    ROUND(SUM(revenue), 2) AS total_revenue,
    COUNT(order_id) AS total_orders
FROM raw_sales_data
GROUP BY zone;

-- Manual refresh: Overwrite with latest metrics
INSERT OVERWRITE TABLE zone_revenue_mv
SELECT 
    zone,
    ROUND(SUM(revenue), 2) AS total_revenue,
    COUNT(order_id) AS total_orders
FROM raw_sales_data
GROUP BY zone;
