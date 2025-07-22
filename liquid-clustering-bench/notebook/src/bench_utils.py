"""Utility functions for timing and logging benchmark queries."""
from pyspark.sql import SparkSession
import time, json

spark = SparkSession.getActiveSession()

RESULT_TABLE = "bench_results"

QUERIES = {
    "Q1": "SELECT SUM(revenue) FROM {} WHERE customer_id = 42",
    "Q2": "SELECT zone, SUM(revenue) FROM {} WHERE year = 2025 GROUP BY zone",
    "Q3": "SELECT * FROM {} WHERE order_ts BETWEEN '2025-05-01' AND '2025-06-30' ORDER BY revenue DESC LIMIT 1000"
}

def run_query(sql_template: str, table: str):
    sql = sql_template.format(table)
    start = time.time()
    spark.sql(sql).collect()
    elapsed = (time.time() - start) * 1000
    return elapsed

def log_result(layout: str, query_id: str, ms: float):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {RESULT_TABLE} (layout STRING, query_id STRING, ms DOUBLE)")
    spark.sql(f"INSERT INTO {RESULT_TABLE} VALUES ('{layout}', '{query_id}', {ms})")
