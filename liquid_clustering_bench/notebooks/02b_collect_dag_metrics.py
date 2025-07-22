from src.bench_utils import QUERIES, run_query
from src.dag_metrics import get_jobs_json, summarize_jobs
import pandas as pd

layouts = {
    "partition": "sales_partitioned",
    "zorder":    "sales_zorder",
    "liquid":    "sales_liquid"
}

out = []
for layout, table in layouts.items():
    for qid, sql_tmpl in QUERIES.items():
        _ = run_query(sql_tmpl, table)
        jobs = get_jobs_json()
        summary = summarize_jobs(jobs[-1:])  # last job
        for (job_id, stage_count, duration, status) in summary:
            out.append((layout, qid, job_id, stage_count, duration, status))

pdf = pd.DataFrame(out, columns=["layout","query_id","job_id","stages","duration_ms","status"])
spark.createDataFrame(pdf).write.mode("append").saveAsTable("bench_dag_stats")
