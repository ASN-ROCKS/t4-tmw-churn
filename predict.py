# Databricks notebook source
# MAGIC %pip install feature-engine scikit-learn pandas==2.2.0 --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow

model = mlflow.sklearn.load_model("models:/t4-churn-tmw/production")
model

# COMMAND ----------

query = """
WITH max_dt AS (

  SELECT max(dtRef) AS dtRef
  FROM sandbox.asn.t4_points_churn_feature_store

)

SELECT *
FROM sandbox.asn.t4_points_churn_feature_store
WHERE dtRef = (SELECT dtRef FROM max_dt)
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
          .option("replaceWhere", f"dtRef = '{dt}'")
          .saveAsTable("sandbox.asn.t4_points_churn_score"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM sandbox.asn.t4_points_churn_score
# MAGIC where idCliente = '2d3d2dce-d353-4961-ad39-46723efe2100'
