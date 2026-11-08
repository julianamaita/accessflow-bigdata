# Databricks notebook source
# MAGIC %md
# MAGIC # AccessFlow · 01 · Setup (rodar uma vez)
# MAGIC Cria o schema `workspace.accessflow`, o Volume `chegada` e as subpastas onde os
# MAGIC lotes da pasta `chegada/` do Airflow serão enviados **pela interface** do Databricks
# MAGIC (Catalog > workspace > accessflow > Volumes > chegada > Upload to this volume).

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.accessflow COMMENT 'AccessFlow - GS 2026.2'")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.accessflow.chegada COMMENT 'Pouso dos arquivos do Airflow'")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

for _, (pasta, _) in FONTES.items():
    dbutils.fs.mkdirs(f"{BASE}/{pasta}")
dbutils.fs.mkdirs(CONTROLE)
display(dbutils.fs.ls(BASE))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segredo do hash (LGPD)
# MAGIC O "sal" usado para pseudonimizar e-mails na Ouro **não fica no código**.
# MAGIC Opção 1 (recomendada), via Databricks CLI:
# MAGIC ```
# MAGIC databricks secrets create-scope accessflow
# MAGIC databricks secrets put-secret accessflow salt_hash
# MAGIC ```
# MAGIC Opção 2: preencher o widget `salt_hash` no topo do notebook 04 na hora de rodar.
