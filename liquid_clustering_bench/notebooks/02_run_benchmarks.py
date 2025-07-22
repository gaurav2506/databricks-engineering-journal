from src.bench_utils import QUERIES, run_query, log_result

layouts = {
    "partition": "sales_partitioned",
    "zorder": "sales_zorder",
    "liquid": "sales_liquid"
}

for layout, table in layouts.items():
    for qid, sql_tmpl in QUERIES.items():
        ms = run_query(sql_tmpl, table)
        log_result(layout, qid, ms)
