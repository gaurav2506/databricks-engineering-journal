import json, time
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
sc = spark.sparkContext

def get_app_ui():
    jurl = sc._jsc.sc().uiWebUrl().get()  
    return jurl

def get_jobs_json():
    import requests
    base = get_app_ui().rsplit('/', 1)[0]  # strip /jobs/
    r = requests.get(f"{base}/api/v1/applications/{sc.applicationId}/jobs")
    return r.json()

def summarize_jobs(jobs):
    rows = []
    for j in jobs:
        sid = j["jobId"]
        stages = j.get("stageIds", [])
        status = j["status"]
        duration = j.get("duration", 0)
        rows.append((sid, len(stages), duration, status))
    return rows

def explain_formatted(sql: str):
    df = spark.sql(sql)
    # Using Spark 3.4+ 'explain' with mode
    return df._sc._jvm.PythonSQLUtils.explainString(df._jdf, "formatted")
