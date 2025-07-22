# Partitioned table
spark.sql("""
CREATE OR REPLACE TABLE sales_partitioned
USING DELTA
PARTITIONED BY (year, month)
AS SELECT * FROM raw_sales_data
""")

# Z‑ORDER table
spark.sql("""
CREATE OR REPLACE TABLE sales_zorder
USING DELTA AS SELECT * FROM raw_sales_data
""")

spark.sql("OPTIMIZE sales_zorder ZORDER BY (customer_id)")

# Liquid‑clustered table
spark.sql("""
CREATE OR REPLACE TABLE sales_liquid
USING DELTA
CLUSTER BY (customer_id)
AS SELECT * FROM raw_sales_data
""")
