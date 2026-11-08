# Databricks notebook source
# MAGIC %md
# MAGIC # AccessFlow · 04 · Ouro (modelo dimensional + LGPD + contrato com o MVP)
# MAGIC
# MAGIC **Fatos**
# MAGIC * `ouro_fato_ocorrencia` — 1 linha por ocorrência (snapshot acumulado: detecção → atribuição → resolução → verificação)
# MAGIC * `ouro_fato_inspecao` — 1 linha por leitura do robô em um trecho (inclusive "sem bloqueio")
# MAGIC
# MAGIC **Dimensões**: `ouro_dim_estabelecimento` (CNES + IBGE), `ouro_dim_trecho`, `ouro_dim_tipo_obstaculo`,
# MAGIC `ouro_dim_responsavel` (pseudonimizada), `ouro_dim_data`
# MAGIC
# MAGIC **LGPD**: nenhum nome, e-mail ou telefone em claro na Ouro. O responsável vira um hash
# MAGIC SHA-256 com sal secreto (permite contar/agrupar sem identificar) + nome/e-mail mascarados.
# MAGIC
# MAGIC **Contrato com o MVP**: as views `ouro_vw_mvp_*` são o que a API do AccessFlow consome.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.text("salt_hash", "", "Sal do hash (se não houver secret)")
try:
    SALT = dbutils.secrets.get(scope="accessflow", key="salt_hash")
except Exception:
    SALT = dbutils.widgets.get("salt_hash")
if not SALT:
    raise ValueError("Defina o secret accessflow/salt_hash ou preencha o widget salt_hash.")

# O hash é calculado via DataFrame (o sal não aparece no texto SQL nem no histórico de consultas).
# A view temporária abaixo é a única porta de entrada das ocorrências na Ouro.
(spark.table(T("prata_ocorrencias"))
   .withColumn("sk_responsavel", F.sha2(F.concat(F.lit(SALT), F.col("responsavel_email")), 256))
   .createOrReplaceTempView("_ocorrencias_pseudonimizadas"))

# COMMAND ----------

# MAGIC %md ## Dimensões

# COMMAND ----------

criar_tabela("ouro_dim_estabelecimento", f"""
  WITH monitorados AS (
    SELECT DISTINCT cnes FROM {T('prata_ocorrencias')}
    UNION SELECT DISTINCT cnes FROM {T('prata_inspecoes')})
  SELECT xxhash64(e.cnes)                       AS sk_estabelecimento,
         e.cnes, e.nome_fantasia, e.codigo_tipo_unidade,
         CASE e.codigo_tipo_unidade
           WHEN 1 THEN 'Posto de saúde' WHEN 2 THEN 'UBS / Centro de saúde'
           WHEN 4 THEN 'Policlínica' WHEN 5 THEN 'Hospital geral'
           WHEN 7 THEN 'Hospital especializado' WHEN 22 THEN 'Consultório'
           WHEN 36 THEN 'Clínica / Centro de especialidades' WHEN 39 THEN 'Apoio diagnóstico e terapia'
           WHEN 62 THEN 'Hospital-dia' WHEN 73 THEN 'Pronto atendimento'
           ELSE 'Outros' END                     AS tipo_unidade,
         e.esfera_administrativa, e.atende_sus, e.bairro, e.endereco,
         e.latitude, e.longitude,
         coalesce(m.municipio, 'Não informado')  AS municipio,
         m.uf, m.codigo_ibge, m.populacao        AS populacao_municipio, m.ano_referencia AS ano_populacao,
         (mo.cnes IS NOT NULL)                   AS monitorado_accessflow
  FROM {T('prata_estabelecimentos')} e
  LEFT JOIN {T('prata_municipios')} m ON m.codigo_municipio_6 = e.codigo_municipio_6
  LEFT JOIN monitorados mo ON mo.cnes = e.cnes""")

criar_tabela("ouro_dim_trecho", f"""
  SELECT xxhash64(id_trecho) AS sk_trecho, id_trecho, cnes,
         max(nome_trecho) AS nome_trecho, max(tipo_trecho) AS tipo_trecho
  FROM (SELECT id_trecho, cnes, nome_trecho, tipo_trecho FROM {T('prata_ocorrencias')}
        UNION ALL
        SELECT id_trecho, cnes, nome_trecho, tipo_trecho FROM {T('prata_inspecoes')})
  GROUP BY id_trecho, cnes""")

criar_tabela("ouro_dim_tipo_obstaculo", f"""
  SELECT xxhash64(tipo_obstaculo) AS sk_tipo_obstaculo, tipo_obstaculo,
         CASE tipo_obstaculo WHEN 'caixa' THEN 'Caixa ou volume' WHEN 'carrinho' THEN 'Carrinho (limpeza/materiais)'
              WHEN 'cadeira' THEN 'Cadeira ou longarina' WHEN 'maca' THEN 'Maca'
              WHEN 'lixeira' THEN 'Lixeira' ELSE 'Outro objeto' END AS descricao,
         CASE WHEN tipo_obstaculo IN ('maca', 'carrinho') THEN 'alta' ELSE 'media' END AS criticidade
  FROM (SELECT DISTINCT tipo_obstaculo FROM {T('prata_ocorrencias')})""")

# Pseudonimização: hash com sal + máscara. O e-mail em claro NÃO sai da Prata.
criar_tabela("ouro_dim_responsavel", f"""
  SELECT DISTINCT sk_responsavel,
         regexp_replace(responsavel_nome, '(\\\\p{{L}})\\\\p{{L}}*', '$1***')        AS nome_mascarado,
         concat(substr(responsavel_email, 1, 1), '***@', split(responsavel_email, '@')[1]) AS email_mascarado
  FROM _ocorrencias_pseudonimizadas WHERE responsavel_email IS NOT NULL""")

criar_tabela("ouro_dim_data", f"""
  WITH limites AS (
    SELECT least(min(to_date(detectado_em)), current_date()) AS ini, current_date() AS fim
    FROM {T('prata_ocorrencias')})
  SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS sk_data, d AS data,
         year(d) AS ano, month(d) AS mes, day(d) AS dia, weekofyear(d) AS semana_ano,
         dayofweek(d) AS dia_semana_num,
         element_at(array('domingo','segunda','terça','quarta','quinta','sexta','sábado'), dayofweek(d)) AS dia_semana,
         dayofweek(d) IN (1, 7) AS fim_de_semana
  FROM limites LATERAL VIEW explode(sequence(ini, fim, interval 1 day)) AS d""")

# COMMAND ----------

# MAGIC %md ## Fatos

# COMMAND ----------

criar_tabela("ouro_fato_ocorrencia", f"""
  SELECT o.id_ocorrencia,
         xxhash64(o.cnes)                    AS sk_estabelecimento,
         xxhash64(o.id_trecho)               AS sk_trecho,
         xxhash64(o.tipo_obstaculo)          AS sk_tipo_obstaculo,
         o.sk_responsavel,
         CAST(date_format(o.detectado_em, 'yyyyMMdd') AS INT) AS sk_data_deteccao,
         o.fonte, o.status, o.confirmado_por_humano, o.confianca, o.distancia_cm,
         o.detectado_em, o.atribuido_em, o.resolvido_em, o.verificado_em,
         round((unix_timestamp(o.atribuido_em) - unix_timestamp(o.detectado_em)) / 60, 1) AS min_ate_atribuicao,
         round((unix_timestamp(o.resolvido_em) - unix_timestamp(o.detectado_em)) / 60, 1) AS min_ate_resolucao,
         round((unix_timestamp(o.verificado_em) - unix_timestamp(o.detectado_em)) / 60, 1) AS min_ate_verificacao,
         o.status IN ('aberta', 'atribuida')                                  AS em_aberto,
         o.status <> 'descartada'                                             AS obstrucao_confirmada,
         o.resolvido_em IS NOT NULL
           AND unix_timestamp(o.resolvido_em) - unix_timestamp(o.detectado_em) <= 30 * 60 AS resolvida_em_ate_30min
  FROM _ocorrencias_pseudonimizadas o""")

criar_tabela("ouro_fato_inspecao", f"""
  SELECT i.id_inspecao,
         xxhash64(i.cnes)        AS sk_estabelecimento,
         xxhash64(i.id_trecho)   AS sk_trecho,
         CAST(date_format(i.inspecionado_em, 'yyyyMMdd') AS INT) AS sk_data,
         i.inspecionado_em, i.resultado, i.distancia_minima_cm, i.id_ocorrencia, i.executado_por
  FROM {T('prata_inspecoes')} i""")

# COMMAND ----------

# MAGIC %md ## Contrato com o MVP (o que a API consome)
# MAGIC | View | Tela do MVP |
# MAGIC |---|---|
# MAGIC | `ouro_vw_mvp_status_publico` | Página pública do cidadão (status do acesso, última inspeção, orientação) |
# MAGIC | `ouro_vw_mvp_painel_estabelecimento` | Cards do painel: trechos, obstruções abertas, tempo pendente |
# MAGIC | `ouro_vw_mvp_ocorrencias_abertas` | Lista de pendências do painel (responsável mascarado) |

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE VIEW {T('ouro_vw_mvp_status_publico')} AS
  WITH ultima AS (
    SELECT sk_trecho, max(inspecionado_em) AS ultima_inspecao
    FROM {T('ouro_fato_inspecao')} GROUP BY sk_trecho),
  aberta AS (
    SELECT sk_trecho, min(detectado_em) AS bloqueado_desde, count(*) AS obstrucoes
    FROM {T('ouro_fato_ocorrencia')} WHERE em_aberto AND obstrucao_confirmada GROUP BY sk_trecho)
  SELECT e.cnes, e.nome_fantasia AS estabelecimento, e.endereco, e.bairro, e.municipio,
         t.nome_trecho AS trecho, t.tipo_trecho,
         CASE WHEN a.obstrucoes > 0 THEN 'Obstrução registrada' ELSE 'Sem bloqueio' END AS status_acesso,
         a.bloqueado_desde, u.ultima_inspecao,
         CASE WHEN a.obstrucoes > 0
              THEN 'Passagem com obstáculo. A equipe da unidade já foi acionada; peça apoio na recepção.'
              ELSE 'Passagem livre na última inspeção.' END AS orientacao
  FROM {T('ouro_dim_trecho')} t
  JOIN {T('ouro_dim_estabelecimento')} e ON e.cnes = t.cnes
  LEFT JOIN ultima u ON u.sk_trecho = t.sk_trecho
  LEFT JOIN aberta a ON a.sk_trecho = t.sk_trecho""")

spark.sql(f"""CREATE OR REPLACE VIEW {T('ouro_vw_mvp_painel_estabelecimento')} AS
  SELECT e.cnes, e.nome_fantasia AS estabelecimento,
         count(DISTINCT t.sk_trecho)                                            AS trechos_monitorados,
         count(DISTINCT CASE WHEN f.em_aberto AND f.obstrucao_confirmada THEN f.id_ocorrencia END) AS obstrucoes_abertas,
         round(max(CASE WHEN f.em_aberto AND f.obstrucao_confirmada
                   THEN (unix_timestamp(current_timestamp()) - unix_timestamp(f.detectado_em)) / 60 END), 0)
                                                                                AS maior_tempo_pendente_min,
         round(avg(CASE WHEN f.obstrucao_confirmada THEN f.min_ate_resolucao END), 1) AS media_min_resolucao
  FROM {T('ouro_dim_estabelecimento')} e
  JOIN {T('ouro_dim_trecho')} t ON t.cnes = e.cnes
  LEFT JOIN {T('ouro_fato_ocorrencia')} f ON f.sk_trecho = t.sk_trecho
  GROUP BY e.cnes, e.nome_fantasia""")

spark.sql(f"""CREATE OR REPLACE VIEW {T('ouro_vw_mvp_ocorrencias_abertas')} AS
  SELECT f.id_ocorrencia, e.cnes, e.nome_fantasia AS estabelecimento, t.nome_trecho AS trecho,
         o.descricao AS obstaculo, o.criticidade, f.fonte, f.status, f.confianca, f.detectado_em,
         round((unix_timestamp(current_timestamp()) - unix_timestamp(f.detectado_em)) / 60, 0) AS minutos_pendente,
         r.nome_mascarado AS responsavel, r.email_mascarado AS contato_responsavel
  FROM {T('ouro_fato_ocorrencia')} f
  JOIN {T('ouro_dim_estabelecimento')} e ON e.sk_estabelecimento = f.sk_estabelecimento
  JOIN {T('ouro_dim_trecho')} t          ON t.sk_trecho = f.sk_trecho
  JOIN {T('ouro_dim_tipo_obstaculo')} o  ON o.sk_tipo_obstaculo = f.sk_tipo_obstaculo
  LEFT JOIN {T('ouro_dim_responsavel')} r ON r.sk_responsavel = f.sk_responsavel
  WHERE f.em_aberto AND f.obstrucao_confirmada""")

# COMMAND ----------

# MAGIC %md ### Evidências (requisitos 07 e 08)

# COMMAND ----------

display(spark.sql(f"""
  SELECT tabela, linhas_depois AS linhas FROM {T('controle_contagens')}
  WHERE camada = 'ouro' AND execucao_id = '{EXECUCAO_ID}' ORDER BY tabela"""))

# COMMAND ----------

# LGPD: prova de que não há e-mail/nome em claro na Ouro
display(spark.sql(f"SELECT * FROM {T('ouro_dim_responsavel')} LIMIT 5"))
display(spark.sql(f"SELECT * FROM {T('ouro_vw_mvp_status_publico')} ORDER BY estabelecimento, trecho"))
