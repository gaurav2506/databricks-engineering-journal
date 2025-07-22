import pandas as pd
import matplotlib.pyplot as plt

pdf = spark.sql("SELECT layout, query_id, avg(ms) as ms FROM bench_results GROUP BY layout, query_id").toPandas()

pivot = pdf.pivot(index="query_id", columns="layout", values="ms")

pivot.plot(kind="bar")
plt.title("Query Latency: Partition vs Z‑Order vs Liquid")
plt.ylabel("Time (ms)")
plt.xticks(rotation=0)
plt.tight_layout()
plt.show()
