# Databricks notebook source
# MAGIC %md
# MAGIC # AccessFlow · 03 · Prata (tratamento, qualidade e MERGE)
# MAGIC Para cada fonte: **tipos corretos → nulos/inválidos tratados → regras de qualidade
# MAGIC (com total barrado por regra) → 1 versão por chave → MERGE INTO pela chave**.
# MAGIC
# MAGIC | Tabela | Chave do MERGE | Regras |
# MAGIC |---|---|---|
# MAGIC | `prata_estabelecimentos` | `cnes` | cnes inválido |
# MAGIC | `prata_municipios` | `codigo_ibge` | população inválida |
# MAGIC | `prata_ocorrencias` | `id_ocorrencia` (+ versão `atualizado_em`) | obrigatórios, confiança, CNES não cadastrado, cronologia, status |
# MAGIC | `prata_inspecoes` | `id_inspecao` | obrigatórios, resultado, CNES não cadastrado |
# MAGIC
# MAGIC Barrados ficam em `prata_quarentena`; contagens em `prata_regras_qualidade`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md ## 1. Estabelecimentos (CNES)

# COMMAND ----------

b = spark.table(T("bronze_cnes"))

lat = col_ou_nulo(b, "latitude_estabelecimento_decimo_grau").cast("double")
lon = col_ou_nulo(b, "longitude_estabelecimento_decimo_grau").cast("double")
dentro_do_brasil = lat.between(-34.0, 5.5) & lon.between(-74.0, -34.0)

est = b.select(
    F.lpad(F.trim(col_ou_nulo(b, "codigo_cnes")), 7, "0").alias("cnes"),
    F.initcap(F.trim(col_ou_nulo(b, "nome_fantasia"))).alias("nome_fantasia"),
    F.initcap(F.trim(col_ou_nulo(b, "nome_razao_social"))).alias("razao_social"),
    col_ou_nulo(b, "codigo_tipo_unidade").cast("int").alias("codigo_tipo_unidade"),
    F.initcap(col_ou_nulo(b, "descricao_esfera_administrativa")).alias("esfera_administrativa"),
    F.lpad(col_ou_nulo(b, "codigo_municipio"), 6, "0").alias("codigo_municipio_6"),
    F.lpad(col_ou_nulo(b, "codigo_uf"), 2, "0").alias("codigo_uf"),
    F.initcap(F.trim(col_ou_nulo(b, "bairro_estabelecimento"))).alias("bairro"),
    F.concat_ws(", ", F.initcap(F.trim(col_ou_nulo(b, "endereco_estabelecimento"))),
                F.trim(col_ou_nulo(b, "numero_estabelecimento"))).alias("endereco"),
    # coordenada fora do Brasil ou 0,0 = desconhecida (tratada como nulo, não barrada)
    F.when(dentro_do_brasil, lat).alias("latitude"),
    F.when(dentro_do_brasil, lon).alias("longitude"),
    (F.upper(col_ou_nulo(b, "estabelecimento_faz_atendimento_ambulatorial_sus")) == "SIM").alias("atende_sus"),
    "_data_coleta", "_arquivo_origem",
)
est = aplicar_regras(est, "prata_estabelecimentos", "cnes", [
    ("cnes_invalido", "código CNES nulo ou fora do padrão de 7 dígitos",
     F.col("cnes").isNull() | ~F.col("cnes").rlike(r"^\d{7}$")),
])
est = ultima_versao(est, "cnes", ["_data_coleta", "_arquivo_origem"])  # CNES é foto diária: fica a mais recente
merge_por_chave(est, "prata_estabelecimentos", ["cnes"])

# COMMAND ----------

# MAGIC %md ## 2. Municípios e população (IBGE)

# COMMAND ----------

bi = spark.table(T("bronze_ibge"))
mun = (bi.select(F.explode("resultados").alias("r"), "_data_coleta", "_arquivo_origem")
         .select(F.explode("r.series").alias("s"), "_data_coleta", "_arquivo_origem")
         .select("s.localidade.id", "s.localidade.nome", F.explode("s.serie").alias("ano", "valor"),
                 "_data_coleta", "_arquivo_origem")
         .select(F.col("id").alias("codigo_ibge"),
                 F.substring("id", 1, 6).alias("codigo_municipio_6"),   # CNES usa 6 dígitos
                 F.trim(F.split("nome", " - ").getItem(0)).alias("municipio"),
                 F.trim(F.split("nome", " - ").getItem(1)).alias("uf"),
                 F.col("ano").cast("int").alias("ano_referencia"),
                 F.when(F.col("valor").rlike(r"^\d+$"), F.col("valor").cast("bigint")).alias("populacao"),
                 "_data_coleta", "_arquivo_origem"))
mun = aplicar_regras(mun, "prata_municipios", "codigo_ibge", [
    ("populacao_invalida", "população nula, não numérica ou <= 0 (IBGE usa '-' e '...')",
     F.col("populacao").isNull() | (F.col("populacao") <= 0)),
])
mun = ultima_versao(mun, "codigo_ibge", ["ano_referencia", "_data_coleta"])
merge_por_chave(mun, "prata_municipios", ["codigo_ibge"])

# COMMAND ----------

# MAGIC %md ## 3. Ocorrências (AccessFlow)
# MAGIC A mesma ocorrência chega em vários dias (uma versão por mudança de status).
# MAGIC O MERGE só atualiza quando `atualizado_em` do lote é mais recente.

# COMMAND ----------

def sub(df, coluna, campo):
    """Campo de objeto aninhado. Com inferColumnTypes=false o Auto Loader entrega o objeto como texto JSON."""
    from pyspark.sql.types import StructType
    if isinstance(df.schema[coluna].dataType, StructType):
        return F.col(f"{coluna}.{campo}")
    return F.get_json_object(F.col(coluna), f"$.{campo}")


STATUS_VALIDOS = ["aberta", "atribuida", "resolvida", "verificada", "descartada"]
FONTES_VALIDAS = ["visao", "robo", "manual"]
cnes_cadastrados = spark.table(T("prata_estabelecimentos")).select(F.col("cnes").alias("_cnes_ok"))

bo = spark.table(T("bronze_ocorrencias")).dropDuplicates(
    [c for c in spark.table(T("bronze_ocorrencias")).columns if not c.startswith("_")])  # duplicata exata na origem
ts = lambda c: F.to_timestamp(col_ou_nulo(bo, c))  # noqa: E731  ISO 8601 com fuso (-03:00)

oc = (bo.select(
        F.trim(col_ou_nulo(bo, "id_ocorrencia")).alias("id_ocorrencia"),
        F.lpad(F.trim(col_ou_nulo(bo, "cnes")), 7, "0").alias("cnes"),
        sub(bo, "trecho", "id").alias("id_trecho"), sub(bo, "trecho", "nome").alias("nome_trecho"),
        sub(bo, "trecho", "tipo").alias("tipo_trecho"),
        F.lower(F.trim(col_ou_nulo(bo, "fonte"))).alias("fonte"),
        F.lower(F.trim(col_ou_nulo(bo, "tipo_obstaculo"))).alias("tipo_obstaculo"),   # " Caixa" -> "caixa"
        col_ou_nulo(bo, "confianca").cast("double").alias("confianca"),
        col_ou_nulo(bo, "distancia_cm").cast("int").alias("distancia_cm"),
        F.lower(F.trim(col_ou_nulo(bo, "status"))).alias("status"),
        col_ou_nulo(bo, "confirmado_por_humano").cast("boolean").alias("confirmado_por_humano"),
        ts("detectado_em").alias("detectado_em"), ts("atribuido_em").alias("atribuido_em"),
        ts("resolvido_em").alias("resolvido_em"), ts("verificado_em").alias("verificado_em"),
        ts("atualizado_em").alias("atualizado_em"),
        # dados pessoais: só o necessário (telefone é descartado já na Prata - minimização LGPD)
        F.initcap(F.trim(sub(bo, "responsavel", "nome"))).alias("responsavel_nome"),
        F.lower(F.trim(sub(bo, "responsavel", "email"))).alias("responsavel_email"),
        "_data_coleta", "_arquivo_origem")
      .withColumn("tipo_obstaculo", F.coalesce("tipo_obstaculo", F.lit("outro")))
      .join(cnes_cadastrados, F.col("cnes") == F.col("_cnes_ok"), "left"))

oc = aplicar_regras(oc, "prata_ocorrencias", "id_ocorrencia", [
    ("obrigatorios_ausentes", "id, CNES, trecho ou data de detecção nulos/ilegíveis",
     F.col("id_ocorrencia").isNull() | F.col("cnes").isNull() | F.col("id_trecho").isNull()
     | F.col("detectado_em").isNull() | F.col("atualizado_em").isNull()),
    ("confianca_fora_0_1", "detecção por visão com confiança nula ou fora de [0, 1]",
     (F.col("fonte") == "visao") & (F.col("confianca").isNull() | ~F.col("confianca").between(0, 1))),
    ("cnes_nao_cadastrado", "CNES não existe em prata_estabelecimentos (integridade)",
     F.col("_cnes_ok").isNull()),
    ("cronologia_invalida", "resolução antes da detecção ou verificação antes da resolução",
     (F.col("resolvido_em") < F.col("detectado_em")) | (F.col("verificado_em") < F.col("resolvido_em"))),
    ("dominio_invalido", "status ou fonte fora da lista permitida",
     F.col("status").isNull() | ~F.col("status").isin(STATUS_VALIDOS)
     | F.col("fonte").isNull() | ~F.col("fonte").isin(FONTES_VALIDAS)),
]).drop("_cnes_ok")

oc = ultima_versao(oc, "id_ocorrencia", ["atualizado_em", "_data_coleta"])
merge_por_chave(oc, "prata_ocorrencias", ["id_ocorrencia"], coluna_versao="atualizado_em")

# COMMAND ----------

# MAGIC %md ## 4. Inspeções (rondas do robô)

# COMMAND ----------

bn = spark.table(T("bronze_inspecoes"))
insp = (bn.select(
          F.trim(col_ou_nulo(bn, "id_inspecao")).alias("id_inspecao"),
          F.lpad(F.trim(col_ou_nulo(bn, "cnes")), 7, "0").alias("cnes"),
          sub(bn, "trecho", "id").alias("id_trecho"), sub(bn, "trecho", "nome").alias("nome_trecho"),
          sub(bn, "trecho", "tipo").alias("tipo_trecho"),
          F.trim(col_ou_nulo(bn, "executado_por")).alias("executado_por"),
          F.to_timestamp(col_ou_nulo(bn, "inspecionado_em")).alias("inspecionado_em"),
          F.lower(F.trim(col_ou_nulo(bn, "resultado"))).alias("resultado"),
          col_ou_nulo(bn, "distancia_minima_cm").cast("int").alias("distancia_minima_cm"),
          F.nullif(F.trim(col_ou_nulo(bn, "id_ocorrencia")), F.lit("")).alias("id_ocorrencia"),
          "_data_coleta", "_arquivo_origem")
        .join(cnes_cadastrados, F.col("cnes") == F.col("_cnes_ok"), "left"))
insp = aplicar_regras(insp, "prata_inspecoes", "id_inspecao", [
    ("obrigatorios_ausentes", "id, trecho ou horário da inspeção nulos",
     F.col("id_inspecao").isNull() | F.col("id_trecho").isNull() | F.col("inspecionado_em").isNull()),
    ("resultado_invalido", "resultado diferente de livre/obstruido",
     F.col("resultado").isNull() | ~F.col("resultado").isin("livre", "obstruido")),
    ("cnes_nao_cadastrado", "CNES não existe em prata_estabelecimentos", F.col("_cnes_ok").isNull()),
]).drop("_cnes_ok")
insp = ultima_versao(insp, "id_inspecao", ["_data_coleta", "_arquivo_origem"])
merge_por_chave(insp, "prata_inspecoes", ["id_inspecao"])

# COMMAND ----------

# MAGIC %md ### Evidência para o relatório (requisito 06)

# COMMAND ----------

display(spark.sql(f"""
  SELECT tabela, regra, descricao, avaliados, barrados
  FROM {T('prata_regras_qualidade')}
  WHERE execucao_id = '{EXECUCAO_ID}' ORDER BY tabela, regra"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT tabela, regra, count(*) AS registros, max(registro) AS exemplo
  FROM {T('prata_quarentena')} GROUP BY tabela, regra ORDER BY tabela, regra"""))
