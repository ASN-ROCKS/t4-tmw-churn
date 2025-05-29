# Databricks notebook source
import datetime

def import_query(path):
    with open(path, 'r') as open_file:
        return open_file.read()

def date_range(start, stop, monthly=False):
    dates = []
    while start <= stop:
        dates.append(start)
        dt = datetime.datetime.strptime(start, '%Y-%m-%d') + datetime.timedelta(days=1)
        start = dt.strftime("%Y-%m-%d")

    if monthly:
        dates = [i for i in dates if i.endswith("01")]
    return dates

query = import_query("feature_store.sql")

dt_start = dbutils.widgets.get("start")
dt_stop = dbutils.widgets.get("stop")

dates = date_range(dt_start, dt_stop, monthly=False)


# COMMAND ----------

for i in dates:
    df = spark.sql(query.format(date=i))
    (df.write.format("delta")
             .mode("overwrite")
             .partitionBy("dtRef")
             .option("overwriteSchema", "true")
             .option("replaceWhere", f"dtRef = '{i}'")
             .saveAsTable("sandbox.asn.t4_points_churn_feature_store"))
