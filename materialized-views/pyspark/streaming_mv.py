from pyspark.sql.functions import sum, count

sales_stream = spark.readStream.format("delta").table("sales_data")

aggregated_stream = sales_stream.groupBy("region").agg(
    sum("sales").alias("total_sales"),
    count("order_id").alias("order_count")
)

aggregated_stream.writeStream.format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .table("materialized_sales_view")
