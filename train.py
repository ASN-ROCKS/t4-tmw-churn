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

# DBTITLE 1,MODIFY
from sklearn import tree
from sklearn import ensemble
from sklearn import metrics
from sklearn import pipeline
from sklearn import naive_bayes
from sklearn import model_selection

# Para avgIntervalDays, medianIntervalDays, maxIntervalDays, stdIntervalDays, qtdInterval28days vamos colocar o máximo

imputer_tail = imputation.EndTailImputer(variables=["avgIntervalDays",
                                                    "medianIntervalDays",
                                                    "maxIntervalDays",
                                                    "stdIntervalDays",
                                                    "qtdInterval28days",], imputation_method='max')

imputer_one = imputation.ArbitraryNumberImputer(variables=["vlIFRBruto",
                                                           "vlIFRPlus1",
                                                           "vlIFRPlus1Case",], arbitrary_number=1)

# COMMAND ----------

# DBTITLE 1,MODEL
model = ensemble.RandomForestClassifier(random_state=42, n_jobs=1)

params = {
    "n_estimators": [100,200,350,500],
    "min_samples_leaf":[10,50,100]
}

grid = model_selection.GridSearchCV(estimator=model,
                                    param_grid=params,
                                    cv=3,
                                    scoring='roc_auc',
                                    n_jobs=2,)


model_pipeline = pipeline.Pipeline(steps=[('imputer_max', imputer_tail),
                                          ('imputer_one', imputer_one),
                                          ('classifier', grid)])

# Step 1: Importa o MLFlow
import mlflow

# Step 2: Define o experimento que o mlflow vai usar
mlflow.set_experiment(experiment_id=1481226312561602)

# Step 3: Iniciar a Run
with mlflow.start_run():

    # Step 4: Habilitar autolog do scikit-learn
    mlflow.sklearn.autolog()

    model_pipeline.fit(X_train, y_train)

    y_pred = model_pipeline.predict(X_train)
    y_proba = model_pipeline.predict_proba(X_train)[:,1]

    acc_treino = metrics.accuracy_score(y_train, y_pred)
    auc_treino = metrics.roc_auc_score(y_train, y_proba)

    pred_test = model_pipeline.predict(X_test)
    prob_test = model_pipeline.predict_proba(X_test)[:,1]

    acc_test = metrics.accuracy_score(y_test, pred_test)
    acc_bal_test = metrics.balanced_accuracy_score(y_test, pred_test)
    auc_test = metrics.roc_auc_score(y_test, prob_test)

    pred_oot = model_pipeline.predict(df_oot[features])
    prob_oot = model_pipeline.predict_proba(df_oot[features])[:,1]

    acc_oot = metrics.accuracy_score(df_oot["flagAtividade"], pred_oot)
    acc_bal_oot = metrics.balanced_accuracy_score(df_oot["flagAtividade"], pred_oot)
    auc_oot = metrics.roc_auc_score(df_oot["flagAtividade"], prob_oot)

    # Step 5: registrar / loggar as métricas
    metricas = {
        "acc_treino":acc_treino,
        "auc_treino":auc_treino,
        "acc_test":acc_test,
        "acc_bal_test":acc_bal_test,
        "auc_test":auc_test,
        "acc_oot":acc_oot,
        "acc_bal_oot":acc_bal_oot,
        "auc_oot":auc_oot,
    }
    mlflow.log_metrics(metricas)

# COMMAND ----------

import pandas as pd

features_model = model_pipeline[:-1].transform(X_train.head(1)).columns.tolist()

feature_importance = pd.Series(model_pipeline[-1].feature_importances_, index=features_model)
feature_importance = feature_importance.sort_values(ascending=False).reset_index()
feature_importance["acum"] = feature_importance[0].cumsum()
feature_importance

# COMMAND ----------

pd.DataFrame(grid.cv_results_).sort_values("rank_test_score")
