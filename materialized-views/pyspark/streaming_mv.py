from pyspark.sql.functions import sum, count, round

# Stream from raw sales data
stream_df = spark.readStream.format("delta").table("raw_sales_data")

# Aggregate revenue and orders by zone
aggregated = stream_df.groupBy("zone").agg(
    round(sum("revenue"), 2).alias("total_revenue"),
    count("order_id").alias("total_orders")
)

# Write to materialized view table with checkpointing
aggregated.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/mnt/checkpoints/zone_mv") \
    .table("zone_revenue_mv")
