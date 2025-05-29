# Databricks notebook source
# MAGIC %pip install feature-engine scikit-learn pandas==2.2.0 --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import datetime


def date_range(start, stop, monthly=False):
    dates = []
    while start <= stop:
        dates.append(start)
        dt = datetime.datetime.strptime(start, '%Y-%m-%d') + datetime.timedelta(days=1)
        start = dt.strftime("%Y-%m-%d")

    if monthly:
        dates = [i for i in dates if i.endswith("01")]
    return dates

dt_start = dbutils.widgets.get("start")
dt_stop = dbutils.widgets.get("stop")

dates = date_range(dt_start, dt_stop)

dt_str = ",".join([f"'{i}'" for i in dates])


# COMMAND ----------

model = mlflow.sklearn.load_model("models:/t4-churn-tmw/production")
model

# COMMAND ----------

query = f"""
WITH max_dt AS (

  SELECT max(dtRef) AS dtRef
  FROM sandbox.asn.t4_points_churn_feature_store

)

SELECT *
FROM sandbox.asn.t4_points_churn_feature_store
WHERE dtRef IN ({dt_str})
"""

data = spark.sql(query).toPandas()
data.head()

# COMMAND ----------

features = model.feature_names_in_
X = data[features]

y_proba = model.predict_proba(X)

# COMMAND ----------

df = data[['dtRef', 'idCliente']].copy()
df[["ChurnScore_0", "ChurnScore_1"]] = y_proba

dt = df["dtRef"].max()

sdf = spark.createDataFrame(df)

(sdf.write.format("delta")
          .mode("overwrite")
          .partitionBy("dtRef")
          .option("overwriteSchema", "true")
          .option("replaceWhere", f"dtRef IN ({dt_str})")
          .saveAsTable("sandbox.asn.t4_points_churn_score"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM sandbox.asn.t4_points_churn_score
# MAGIC where idCliente = '2d3d2dce-d353-4961-ad39-46723efe2100'
