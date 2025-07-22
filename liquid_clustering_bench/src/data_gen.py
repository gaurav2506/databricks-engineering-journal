"""Synthetic data generator for the benchmark."""
import pyspark.sql.functions as F

def generate_sales_data(rows: int = 500_000_000):
    df = (spark.range(rows)
           .withColumn("customer_id", (F.rand() * 1e6).cast("long"))
           .withColumn("product_id", (F.rand() * 1e5).cast("long"))
           .withColumn("zone", F.expr("CASE WHEN rand() < 0.3 THEN 'APAC' WHEN rand()<0.6 THEN 'EMEA' ELSE 'AMER' END"))
           .withColumn("revenue", F.round(F.rand()*500, 2))
           .withColumn("order_ts", F.expr("current_timestamp() - INTERVAL int(rand()*365) DAY"))
           .withColumn("year", F.year("order_ts"))
           .withColumn("month", F.month("order_ts")))
    return df
