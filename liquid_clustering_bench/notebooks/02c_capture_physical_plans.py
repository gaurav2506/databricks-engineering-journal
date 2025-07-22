from src.bench_utils import QUERIES
from src.dag_metrics import explain_formatted

layouts = {
    "partition": "sales_partitioned",
    "zorder":    "sales_zorder",
    "liquid":    "sales_liquid"
}

for layout, table in layouts.items():
    for qid, tmpl in QUERIES.items():
        sql = tmpl.format(table)
        plan = explain_formatted(sql)
        spark.sql("""
          CREATE TABLE IF NOT EXISTS bench_plans
          (layout STRING, query_id STRING, plan STRING)
        """)
        spark.sql(f"""INSERT INTO bench_plans VALUES ('{layout}','{qid}', '{plan.replace("'", "''")}')""")
