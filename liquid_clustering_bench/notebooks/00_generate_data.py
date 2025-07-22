from src.data_gen import generate_sales_data

df = generate_sales_data()

df.write.format("delta").mode("overwrite").saveAsTable("raw_sales_data")
