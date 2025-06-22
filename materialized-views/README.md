# Materialized Views in Databricks — A Practical Guide

In many modern data platforms, **materialized views** (MVs) are a staple for performance optimization. While platforms like Snowflake support native `CREATE MATERIALIZED VIEW` commands, Databricks doesn't — but there's a workaround.

Let's simulate materialized views using **Delta Tables**, **Delta Live Tables (DLT)**, and **Structured Streaming** in Databricks.

---

## What’s a Materialized View?

A materialized view is essentially a **precomputed, stored result of a query**. It saves time and computation by avoiding repetitive recalculations.

Think of it as caching the result of a query, but persisting it as a physical table, with the option to refresh it regularly or automatically.

Use cases:

* Aggregated reports (e.g., revenue by region)
* Frequently accessed dashboards
* Real-time anomaly tracking
* Feature tables for ML pipelines

---

## Implementation Approaches in Databricks

Let’s look at three practical ways to simulate MVs in Databricks:


### 1. **Batch Materialized Views with Delta Tables**

You can store the result of a transformation into a Delta Table and refresh it periodically using a scheduled job.

```sql
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
```

Schedule this SQL logic with a **Databricks Job** every hour, day, or as needed.

---

### 2. **Real-Time Materialized Views with Structured Streaming**

If your data arrives continuously (IoT, event logs, transactions), you can maintain a **live materialized view** using streaming.


```python
from pyspark.sql.functions import sum, count, round

# Stream from raw sales data
df = spark.readStream.format("delta").table("raw_sales_data")

# Aggregate revenue and orders by zone
agg = df.groupBy("zone").agg(
    round(sum("revenue"), 2).alias("total_revenue"),
    count("order_id").alias("total_orders")
)

# Write to MV with checkpointing
agg.writeStream \
   .format("delta") \
   .outputMode("complete") \
   .option("checkpointLocation", "/mnt/checkpoints/zone_mv") \
   .table("zone_revenue_mv")
```

Ensure that the input table is an **append-only Delta Table** for consistent streaming.

---

### 3. **Auto-Refreshed Views with Delta Live Tables (DLT)**

DLT pipelines give you managed, auto-updating views without manually scheduling refresh jobs.

```python
import dlt
from pyspark.sql.functions import sum, count, round

@dlt.table(
    comment "Auto-updating MV of revenue and orders by zone"
)
def zone_revenue_mv():
    df = spark.read("raw_sales_data")
    return df.groupBy("zone").agg(
        round(sum("revenue"), 2).alias("total_revenue"),
        count("order_id").alias("total_orders")
    )
```

This is great for pipelines where you want your views to **self-heal and stay current**.


---

## Summing up

Materialized views in Databricks are a **design pattern**, not a command. With Delta Lake’s flexibility and Spark’s power, you can easily mimic and improve on traditional MVs:

| Approach             | Suitable For           | Auto Refresh?  |
| -------------------- | ---------------------- | -------------- |
| Delta Table          | Scheduled reports      | NO (manual job) |
| Structured Streaming | Real-time dashboards   | YES              |
| Delta Live Tables    | Pipelines + governance | YES             |


