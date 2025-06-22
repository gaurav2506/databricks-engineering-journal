import dlt
from pyspark.sql.functions import sum, count, round

@dlt.table(
    comment="Materialized view of revenue by zone, auto-updated with DLT"
)
def zone_revenue_mv():
    df = spark.read("raw_sales_data")
    return df.groupBy("zone").agg(
        round(sum("revenue"), 2).alias("total_revenue"),
        count("order_id").alias("total_orders")
    )
