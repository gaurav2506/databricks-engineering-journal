# Liquid‑Clustering‑Benchmark

This repository measures query‑time performance of three Delta Lake layouts in Databricks 13.3–14.x:

1. **Hive‑style partitioning** — `PARTITIONED BY (year, month)`
2. **Z‑ORDER** — classic `OPTIMIZE … ZORDER BY (customer_id)`
3. **Liquid Clustering** (GA June 2025) — `CLUSTER BY (customer_id)` with automatic reclustering

### Quick Start (Databricks Workspaces)
```bash
# clone to your DBFS, then in a notebook cell:
%sh
pip install databricks‑sdk matplotlib
