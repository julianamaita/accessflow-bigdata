"""Teste local (sem Databricks) da lógica Bronze -> Prata -> Ouro -> 5 consultas.

Executa as células dos notebooks em um PySpark local. O que é exclusivo do
Databricks é emulado: Auto Loader (lê só arquivos ainda não carregados),
MERGE/CREATE OR REPLACE (Delta) e dbutils. Serve para pegar erro de lógica
antes de subir; a evidência da entrega continua sendo a execução no Databricks.

Uso:  python -I tests/teste_local_spark.py <pasta_chegada>
"""
import re
import shutil
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

RAIZ = Path(__file__).resolve().parents[1]
NB = RAIZ / "databricks"
CHEGADA = Path(sys.argv[1])

shutil.rmtree("/tmp/accessflow-wh", ignore_errors=True)
base = (SparkSession.builder.master("local[2]").appName("accessflow-teste")
        .config("spark.sql.warehouse.dir", "/tmp/accessflow-wh")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false").getOrCreate())
base.sparkContext.setLogLevel("ERROR")
base.sql("DROP DATABASE IF EXISTS accessflow CASCADE")
base.sql("CREATE DATABASE accessflow")


class SparkLocal:
    """Repassa tudo ao Spark real, emulando DELETE (indisponível sem Delta)."""
    def __getattr__(self, nome):
        return getattr(base, nome)

    def sql(self, q):
        m = re.match(r"\s*DELETE FROM (\S+) WHERE (.+)$", q, re.S)
        if m:
            tabela, cond = m.groups()
            resto = base.table(tabela).where(f"NOT ({cond})").localCheckpoint()
            resto.write.mode("overwrite").saveAsTable(tabela)
            return None
        return base.sql(q)


class _Fs:
    def mkdirs(self, *_): pass
    def ls(self, *_): return []


class _Widgets:
    def text(self, *_): pass
    def get(self, _): return "sal-de-teste-local"


class _Secrets:
    def get(self, **_): raise RuntimeError("sem secrets localmente")


class DbUtils:
    fs, widgets, secrets = _Fs(), _Widgets(), _Secrets()


def display(df):
    if df is not None:
        df.show(30, truncate=60)


ns = {"spark": SparkLocal(), "dbutils": DbUtils(), "display": display}


def celulas(arquivo: Path):
    texto = arquivo.read_text(encoding="utf-8")
    for cel in texto.split("# COMMAND ----------")[0:]:
        linhas = [l for l in cel.splitlines() if not l.startswith("# Databricks notebook source")]
        if all(l.startswith("# MAGIC") or not l.strip() for l in linhas):
            continue
        yield "\n".join(linhas)


def rodar(arquivo: Path, pular=()):
    for cel in celulas(arquivo):
        if any(p in cel for p in pular):
            continue
        exec(compile(cel, str(arquivo), "exec"), ns)


# ---- 00_config com catálogo local
cfg = (NB / "00_config.py").read_text(encoding="utf-8").replace('CATALOGO = "workspace"', "CATALOGO = None")
for cel in cfg.split("# COMMAND ----------"):
    if not all(l.startswith("# MAGIC") or not l.strip() or l.startswith("# Databricks") for l in cel.splitlines()):
        exec(cel, ns)


def merge_local(df_novo, tabela, chaves, coluna_versao=None):
    alvo = ns["T"](tabela)
    antes = ns["contar"](tabela)
    novo = df_novo.withColumn("_ordem", F.lit(1))
    if base.catalog.tableExists(alvo):
        atual = base.table(alvo).withColumn("_ordem", F.lit(0))
        novo = atual.unionByName(novo)
    ordem = ([F.col(coluna_versao).desc_nulls_last()] if coluna_versao else []) + [F.col("_ordem").desc()]
    w = Window.partitionBy(*chaves).orderBy(*ordem)
    res = novo.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn", "_ordem").localCheckpoint()
    res.write.mode("overwrite").saveAsTable(alvo)
    ns["registrar_contagem"]("prata", tabela, antes, ns["contar"](tabela), obs=f"MERGE(emulado) {chaves}")


def criar_local(nome, select_sql):
    antes = ns["contar"](nome)
    df = base.sql(select_sql).localCheckpoint()
    base.sql(f"DROP TABLE IF EXISTS {ns['T'](nome)}")
    df.write.saveAsTable(ns["T"](nome))
    ns["registrar_contagem"]("ouro", nome, antes, ns["contar"](nome))


ns["merge_por_chave"], ns["criar_tabela"] = merge_local, criar_local

# ---- Bronze (emulação do Auto Loader: só arquivos novos)
exec([c for c in celulas(NB / "02_bronze.py") if "SCHEMA_IBGE = " in c][0], ns)
carregados: set[str] = set()


def bronze_local(datas: list[str]):
    for fonte, (pasta, tabela) in ns["FONTES"].items():
        antes = ns["contar"](tabela)
        novos = [str(p) for p in sorted((CHEGADA / pasta).glob("*.json"))
                 if any(d in p.name for d in datas) and str(p) not in carregados]
        if novos:
            leitor = base.read.option("multiLine", "true").schema(ns["SCHEMA_IBGE"]) if fonte == "ibge" \
                else base.read.option("primitivesAsString", "true")
            df = (leitor.json(novos)
                  .withColumn("_arquivo_origem", F.input_file_name())
                  .withColumn("_data_coleta", F.to_date(F.regexp_extract("_arquivo_origem", r"(\d{4}-\d{2}-\d{2})", 1)))
                  .withColumn("_carregado_em", F.current_timestamp()))
            if base.catalog.tableExists(ns["T"](tabela)):
                df = df.select(*base.table(ns["T"](tabela)).columns) if set(base.table(ns["T"](tabela)).columns) <= set(df.columns) else df
            df.write.mode("append").option("mergeSchema", "true").saveAsTable(ns["T"](tabela))
            carregados.update(novos)
        depois = ns["contar"](tabela)
        arquivos = base.table(ns["T"](tabela)).select("_arquivo_origem").distinct().count() if depois else 0
        ns["registrar_contagem"]("bronze", tabela, antes, depois, arquivos)


datas = sorted({re.search(r"\d{4}-\d{2}-\d{2}", p.name).group(0) for p in CHEGADA.rglob("*.json")})
meio = len(datas) // 2
print("\n=== LOTE 1:", datas[:meio]); bronze_local(datas[:meio])
print("\n=== LOTE 2:", datas[meio:]); bronze_local(datas[meio:])
print("\n=== REEXECUÇÃO sem arquivos novos"); bronze_local(datas)

print("\n=== PRATA"); rodar(NB / "03_prata.py", pular=("%run",))
print("\n=== PRATA (2ª execução: MERGE não pode duplicar)"); rodar(NB / "03_prata.py", pular=("%run", "display("))
print("\n=== OURO"); rodar(NB / "04_ouro.py", pular=("%run",))

print("\n=== 5 CONSULTAS")
sql = (NB / "05_consultas_mvp.sql").read_text(encoding="utf-8").replace("workspace.accessflow.", "accessflow.")
for i, cel in enumerate([c for c in sql.split("-- COMMAND ----------")
                         if c.strip() and not all(l.startswith("-- ") or not l.strip() for l in c.splitlines())], 1):
    print(f"\n--- Consulta {i}")
    base.sql(cel.strip().rstrip(";")).show(20, truncate=45)

print("\nVerificação LGPD (não pode haver e-mail na Ouro):")
for t in [r.tableName for r in base.sql("SHOW TABLES IN accessflow").collect() if r.tableName.startswith("ouro_")]:
    cols = base.table(f"accessflow.{t}").columns
    bad = [c for c in cols if "email" in c and "mascarado" not in c]
    assert not bad, f"{t} expõe {bad}"
    if "contato_responsavel" not in cols and "email_mascarado" not in cols:
        txt = base.table(f"accessflow.{t}").select(F.to_json(F.struct("*")).alias("j")).where(F.col("j").contains("@exemplo")).count()
        assert txt == 0, f"{t} contém e-mail em claro"
print("OK")
