# Databricks notebook source
# MAGIC %pip install feature-engine scikit-learn pandas==2.2.0 --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from sklearn import model_selection
import pandas as pd
from feature_engine import imputation

df = spark.table("sandbox.asn.t4_abt_points_churn").toPandas()

# COMMAND ----------

# DBTITLE 1,SAMPLE
df_oot = df[df["dtRef"] == df["dtRef"].max()]

df_train = df[df["dtRef"] != df["dtRef"].max()]

features = df_train.columns.tolist()[2:-2]
X = df_train[features]
y = df_train["flagAtividade"]

X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y,
                                                                    test_size=0.2,
                                                                    random_state=42,
                                                                    stratify=y)

print("Taxa da variável resposta Treino:", y_train.mean())
print("Taxa da variável resposta Teste:", y_test.mean())

df_medias = pd.DataFrame()
df_medias["Treino"] = X_train.mean()
df_medias["Test"] = X_test.mean()
df_medias["diff_abs"] = df_medias["Treino"] - df_medias["Test"]
df_medias["diff_rel"] = 1- df_medias["Treino"] / df_medias["Test"]
print(df_medias)

# COMMAND ----------

X_train.isna().mean()

# Para vlIFRBruto, vlIFRPlus1 e vlIFRPlus1Case vamos deixar as árvore decidirem
# Para avgIntervalDays, medianIntervalDays, maxIntervalDays, stdIntervalDays, qtdInterval28days vamos colocar o máximo

# COMMAND ----------

X_train.describe().T

# COMMAND ----------

df_X_y = X_train.copy()
df_X_y["flagAtividade"] = y
df_X_y.groupby("flagAtividade").median().T

# COMMAND ----------

# Para avgIntervalDays, medianIntervalDays, maxIntervalDays, stdIntervalDays, qtdInterval28days vamos colocar o máximo

imputer_tail = imputation.EndTailImputer(variables=["avgIntervalDays",
                                                    "medianIntervalDays",
                                                    "maxIntervalDays",
                                                    "stdIntervalDays",
                                                    "qtdInterval28days",], imputation_method='max')

imputer_tail.fit(X_train, y_train)

# COMMAND ----------

X_train_transform = imputer_tail.transform(X_train)

# COMMAND ----------

from sklearn import tree
from sklearn import ensemble
from sklearn import metrics

# COMMAND ----------

model = tree.DecisionTreeClassifier(random_state=42, min_weight_fraction_leaf=0.05)
model.fit(X_train_transform, y_train)

y_pred = model.predict(X_train_transform)
y_proba = model.predict_proba(X_train_transform)[:,1]

acc_treino = metrics.accuracy_score(y_train, y_pred)
print("taxa de Acurácia em Treino", acc_treino)

auc_treino = metrics.roc_auc_score(y_train, y_proba)
print("taxa de AUC em Treino", auc_treino)

# COMMAND ----------

X_test_transform = imputer_tail.transform(X_test)
pred_test = model.predict(X_test_transform)
prob_test = model.predict_proba(X_test_transform)[:,1]

acc_test = metrics.accuracy_score(y_test, pred_test)
print("taxa de Acurácia em test", acc_test)

acc_bal_test = metrics.balanced_accuracy_score(y_test, pred_test)
print("taxa de Acurácia Balanceada em test", acc_bal_test)

auc_test = metrics.roc_auc_score(y_test, prob_test)
print("taxa de AUC em test", auc_test)

# COMMAND ----------

X_oot_transform = imputer_tail.transform(df_oot[features])
pred_oot = model.predict(X_oot_transform)
prob_oot = model.predict_proba(X_oot_transform)[:,1]

acc_oot = metrics.accuracy_score(df_oot["flagAtividade"], pred_oot)
print("taxa de Acurácia em oot", acc_oot)

acc_bal_oot = metrics.balanced_accuracy_score(df_oot["flagAtividade"], pred_oot)
print("taxa de Acurácia Balanceada em oot", acc_bal_oot)

auc_oot = metrics.roc_auc_score(df_oot["flagAtividade"], prob_oot)
print("taxa de AUC em oot", auc_oot)

# COMMAND ----------


