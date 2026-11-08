# Databricks notebook source
# MAGIC %md
# MAGIC # AccessFlow · 00 · Configuração comum
# MAGIC Chamado pelos outros notebooks com `%run ./00_config`. Define nomes de tabelas,
# MAGIC caminhos do Volume e funções auxiliares (MERGE, regras de qualidade, contagens).

# COMMAND ----------

import json
import uuid
from datetime import datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

CATALOGO = "workspace"          # catálogo padrão do Databricks Free Edition
SCHEMA = "accessflow"
VOLUME = "chegada"
BASE = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"
CONTROLE = f"{BASE}/_controle"   # checkpoints e schemas do Auto Loader

FONTES = {
    # fonte: (subpasta no Volume, tabela bronze)
    "cnes": ("cnes", "bronze_cnes"),
    "ibge": ("ibge", "bronze_ibge"),
    "ocorrencias": ("accessflow_ocorrencias", "bronze_ocorrencias"),
    "inspecoes": ("accessflow_inspecoes", "bronze_inspecoes"),
}

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
EXECUCAO_ID = f"{datetime.now():%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}"


def T(nome: str) -> str:
    """Nome completo da tabela: workspace.accessflow.<nome>."""
    return f"{CATALOGO}.{SCHEMA}.{nome}" if CATALOGO else f"{SCHEMA}.{nome}"


def contar(tabela: str) -> int:
    return spark.table(T(tabela)).count() if spark.catalog.tableExists(T(tabela)) else 0

# COMMAND ----------

# Tabelas de controle: contagens por execução e resultado das regras de qualidade.
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T('controle_contagens')} (
  execucao_id STRING, camada STRING, tabela STRING, linhas_antes BIGINT, linhas_depois BIGINT,
  arquivos_distintos BIGINT, observacao STRING, executado_em TIMESTAMP)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T('prata_regras_qualidade')} (
  execucao_id STRING, tabela STRING, regra STRING, descricao STRING,
  avaliados BIGINT, barrados BIGINT, executado_em TIMESTAMP)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T('prata_quarentena')} (
  tabela STRING, regra STRING, chave STRING, registro STRING, arquivo_origem STRING,
  execucao_id STRING, executado_em TIMESTAMP)""")


def registrar_contagem(camada, tabela, antes, depois, arquivos=None, obs=""):
    linha = [(EXECUCAO_ID, camada, tabela, int(antes), int(depois),
              None if arquivos is None else int(arquivos), obs, datetime.now())]
    spark.createDataFrame(linha, spark.table(T("controle_contagens")).schema) \
        .write.mode("append").saveAsTable(T("controle_contagens"))
    print(f"[{camada}] {tabela}: antes={antes} depois={depois} (+{depois - antes}) arquivos={arquivos} {obs}")

# COMMAND ----------

def aplicar_regras(df: DataFrame, tabela: str, chave: str, regras: list):
    """Aplica regras de qualidade em ordem. Cada regra = (nome, descrição, condição_de_INVALIDO).

    Um registro é barrado pela PRIMEIRA regra que violar. Grava a contagem de cada
    regra em prata_regras_qualidade e os barrados em prata_quarentena.
    Retorna apenas os registros válidos.
    """
    expr_regra = F.lit(None).cast("string")
    for nome, _, cond in reversed(regras):
        expr_regra = F.when(F.coalesce(cond, F.lit(False)), F.lit(nome)).otherwise(expr_regra)
    marcado = df.withColumn("_regra_violada", expr_regra).cache()
    avaliados = marcado.count()
    contagens = {r["_regra_violada"]: r["count"] for r in
                 marcado.where("_regra_violada IS NOT NULL").groupBy("_regra_violada").count().collect()}
    agora = datetime.now()
    linhas = [(EXECUCAO_ID, tabela, nome, desc, avaliados, int(contagens.get(nome, 0)), agora)
              for nome, desc, _ in regras]
    spark.createDataFrame(linhas, spark.table(T("prata_regras_qualidade")).schema) \
        .write.mode("append").saveAsTable(T("prata_regras_qualidade"))

    barrados = marcado.where("_regra_violada IS NOT NULL")
    colunas = [c for c in barrados.columns if c != "_regra_violada"]
    quarentena = barrados.select(
        F.lit(tabela).alias("tabela"), F.col("_regra_violada").alias("regra"),
        F.col(chave).cast("string").alias("chave"),
        F.to_json(F.struct(*colunas)).alias("registro"),
        F.col("_arquivo_origem").alias("arquivo_origem"),
        F.lit(EXECUCAO_ID).alias("execucao_id"), F.current_timestamp().alias("executado_em"))
    # idempotente: a quarentena da tabela é recalculada a cada execução
    spark.sql(f"DELETE FROM {T('prata_quarentena')} WHERE tabela = '{tabela}'")
    quarentena.write.mode("append").saveAsTable(T("prata_quarentena"))

    for nome, desc, _ in regras:
        print(f"  regra {nome:<28} barrou {contagens.get(nome, 0):>5}  ({desc})")
    print(f"  avaliados={avaliados}  válidos={avaliados - sum(contagens.values())}")
    return marcado.where("_regra_violada IS NULL").drop("_regra_violada")

# COMMAND ----------

def merge_por_chave(df_novo: DataFrame, tabela: str, chaves: list, coluna_versao: str = None):
    """Upsert na Prata com MERGE INTO pela chave de negócio.

    Se coluna_versao for informada, só atualiza quando a versão nova for mais recente
    (ocorrências chegam várias vezes, uma por mudança de status).
    """
    alvo = T(tabela)
    if not spark.catalog.tableExists(alvo):
        df_novo.limit(0).write.saveAsTable(alvo)
    df_novo.createOrReplaceTempView("_lote_merge")
    on = " AND ".join(f"alvo.{c} = novo.{c}" for c in chaves)
    cond = f" AND novo.{coluna_versao} >= alvo.{coluna_versao}" if coluna_versao else ""
    antes = contar(tabela)
    spark.sql(f"""
        MERGE INTO {alvo} AS alvo
        USING _lote_merge AS novo
        ON {on}
        WHEN MATCHED{cond} THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    registrar_contagem("prata", tabela, antes, contar(tabela), obs=f"MERGE por {chaves}")


def criar_tabela(nome: str, select_sql: str):
    """Ouro: recria a tabela a partir da Prata (determinístico, rodar de novo dá o mesmo resultado)."""
    antes = contar(nome)
    spark.sql(f"CREATE OR REPLACE TABLE {T(nome)} AS {select_sql}")
    registrar_contagem("ouro", nome, antes, contar(nome))


def ultima_versao(df: DataFrame, chave: str, ordem: list) -> DataFrame:
    """Mantém 1 linha por chave (a mais recente) dentro do lote."""
    w = Window.partitionBy(chave).orderBy(*[F.col(c).desc_nulls_last() for c in ordem])
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def col_ou_nulo(df: DataFrame, nome: str):
    """Tolera campo ausente na fonte (ex.: a API do CNES mudou um nome de coluna)."""
    return F.col(f"`{nome}`") if nome in df.columns else F.lit(None).cast("string")
