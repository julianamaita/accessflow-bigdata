# Databricks notebook source
# MAGIC %md
# MAGIC # AccessFlow · 02 · Bronze (Auto Loader)
# MAGIC Lê do Volume **apenas os arquivos novos** de cada fonte e grava o dado bruto em Delta.
# MAGIC
# MAGIC * `cloudFiles` guarda no checkpoint quais arquivos já entraram: no 2º lote só os
# MAGIC   arquivos novos são lidos e **rodar de novo não duplica nada**.
# MAGIC * `trigger(availableNow=True)`: processa o que houver e para (modo lote, compatível com serverless).
# MAGIC * Colunas técnicas: `_arquivo_origem`, `_data_coleta` (da data no nome do arquivo) e `_carregado_em`.
# MAGIC * Bronze não trata nada: tudo como string (`inferColumnTypes=false`) e campos inesperados vão para `_rescued_data`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType

# A resposta do IBGE é um JSON aninhado cuja chave é o ano ({"2025": "12000000"}).
# Schema explícito com MAP evita que um ano novo quebre a leitura.
SCHEMA_IBGE = StructType([
    StructField("id", StringType()), StructField("variavel", StringType()),
    StructField("unidade", StringType()),
    StructField("resultados", ArrayType(StructType([
        StructField("series", ArrayType(StructType([
            StructField("localidade", StructType([
                StructField("id", StringType()), StructField("nome", StringType())])),
            StructField("serie", MapType(StringType(), StringType())),
        ]))),
    ]))),
])


def carregar_bronze(fonte: str) -> None:
    pasta, tabela = FONTES[fonte]
    antes = contar(tabela)
    leitor = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", f"{CONTROLE}/schema_{fonte}"))
    if fonte == "ibge":
        leitor = leitor.option("multiLine", "true").schema(SCHEMA_IBGE)
    else:  # NDJSON: um registro por linha
        leitor = (leitor.option("cloudFiles.inferColumnTypes", "false")
                  .option("cloudFiles.schemaEvolutionMode", "rescue"))
    consulta = (leitor.load(f"{BASE}/{pasta}")
                .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
                .withColumn("_data_coleta", F.to_date(
                    F.regexp_extract(F.col("_metadata.file_name"), r"(\d{4}-\d{2}-\d{2})", 1)))
                .withColumn("_carregado_em", F.current_timestamp())
                .writeStream
                .option("checkpointLocation", f"{CONTROLE}/checkpoint_{fonte}")
                .trigger(availableNow=True)
                .toTable(T(tabela)))
    consulta.awaitTermination()
    depois = contar(tabela)
    arquivos = spark.table(T(tabela)).select("_arquivo_origem").distinct().count()
    registrar_contagem("bronze", tabela, antes, depois, arquivos)

# COMMAND ----------

for fonte in FONTES:
    carregar_bronze(fonte)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evidência para o relatório (requisito 05)
# MAGIC Rode este notebook (1) após o 1º lote, (2) após o 2º lote e (3) de novo sem enviar nada.
# MAGIC Na 3ª execução `linhas_depois = linhas_antes`: nada duplicou.

# COMMAND ----------

display(spark.sql(f"""
  SELECT executado_em, tabela, linhas_antes, linhas_depois,
         linhas_depois - linhas_antes AS novas, arquivos_distintos
  FROM {T('controle_contagens')} WHERE camada = 'bronze'
  ORDER BY tabela, executado_em"""))

# COMMAND ----------

# Arquivos por data de coleta (mostra os dois lotes de dias diferentes)
display(spark.sql(f"""
  SELECT 'ocorrencias' AS fonte, _data_coleta, count(*) AS linhas, min(_carregado_em) AS carregado_em
  FROM {T('bronze_ocorrencias')} GROUP BY ALL
  UNION ALL
  SELECT 'cnes', _data_coleta, count(*), min(_carregado_em) FROM {T('bronze_cnes')} GROUP BY ALL
  ORDER BY fonte, _data_coleta"""))

# COMMAND ----------

# Estado interno do Auto Loader: cada arquivo aparece uma única vez
display(spark.sql(f"SELECT path, commit_time FROM cloud_files_state('{CONTROLE}/checkpoint_ocorrencias') ORDER BY path"))
