-- Databricks notebook source
-- MAGIC %md
-- MAGIC # AccessFlow · 05 · As 5 consultas do MVP
-- MAGIC Cada consulta responde a uma pergunta que o produto mostra ao cidadão ou ao estabelecimento.
-- MAGIC A consulta 5 é a de **impacto**. Copie resultado + interpretação para o relatório.

-- COMMAND ----------

-- Horários no fuso de São Paulo (o padrão da sessão SQL é UTC)
SET TIME ZONE 'America/Sao_Paulo';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 1 · Cidadão: "A passagem acessível desta unidade está livre agora?"
-- MAGIC Alimenta a **página pública**: status de cada trecho, última inspeção e orientação.

-- COMMAND ----------

SELECT estabelecimento, trecho, status_acesso,
       date_format(ultima_inspecao, 'dd/MM HH:mm') AS ultima_inspecao,
       date_format(bloqueado_desde, 'dd/MM HH:mm') AS bloqueado_desde,
       orientacao
FROM workspace.accessflow.ouro_vw_mvp_status_publico
ORDER BY estabelecimento, status_acesso DESC, trecho;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 2 · Estabelecimento: "O que está pendente e há quanto tempo?"
-- MAGIC Alimenta os **cards do painel** (trechos, obstruções abertas, tempo pendente).

-- COMMAND ----------

SELECT p.estabelecimento, p.trechos_monitorados, p.obstrucoes_abertas,
       coalesce(p.maior_tempo_pendente_min, 0) AS maior_tempo_pendente_min,
       p.media_min_resolucao,
       CASE WHEN p.obstrucoes_abertas = 0 THEN 'ok'
            WHEN p.maior_tempo_pendente_min > 30 THEN 'atrasada (>30 min)'
            ELSE 'em atendimento' END AS situacao
FROM workspace.accessflow.ouro_vw_mvp_painel_estabelecimento p
ORDER BY p.obstrucoes_abertas DESC, maior_tempo_pendente_min DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 3 · Operação: "Quais obstáculos demoram mais e qual fonte detecta melhor?"
-- MAGIC Tempo até resolução por tipo de obstáculo e fonte, e taxa de falso positivo
-- MAGIC (ocorrência descartada na confirmação humana) — mede a qualidade da visão computacional e do robô.

-- COMMAND ----------

SELECT o.descricao AS obstaculo, f.fonte,
       count(*)                                                        AS deteccoes,
       round(100 * avg(CASE WHEN f.status = 'descartada' THEN 1 ELSE 0 END), 1) AS pct_falso_positivo,
       round(avg(f.min_ate_resolucao), 1)                              AS media_min_resolucao,
       round(percentile_approx(f.min_ate_resolucao, 0.5), 1)           AS mediana_min_resolucao,
       round(percentile_approx(f.min_ate_resolucao, 0.9), 1)           AS p90_min_resolucao
FROM workspace.accessflow.ouro_fato_ocorrencia f
JOIN workspace.accessflow.ouro_dim_tipo_obstaculo o USING (sk_tipo_obstaculo)
GROUP BY o.descricao, f.fonte
ORDER BY media_min_resolucao DESC NULLS LAST;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 4 · Prevenção: "Onde e quando os bloqueios se repetem?"
-- MAGIC Trechos reincidentes e o período do dia com mais ocorrências — orienta ação preventiva
-- MAGIC (ex.: mudar o local de descarga de materiais) em vez de só remover o obstáculo.

-- COMMAND ----------

WITH base AS (
  SELECT e.nome_fantasia AS estabelecimento, t.nome_trecho AS trecho, f.sk_data_deteccao, f.min_ate_resolucao,
         CASE WHEN hour(f.detectado_em) < 12 THEN 'manhã'
              WHEN hour(f.detectado_em) < 17 THEN 'tarde' ELSE 'noite' END AS periodo
  FROM workspace.accessflow.ouro_fato_ocorrencia f
  JOIN workspace.accessflow.ouro_dim_trecho t ON t.sk_trecho = f.sk_trecho
  JOIN workspace.accessflow.ouro_dim_estabelecimento e ON e.sk_estabelecimento = f.sk_estabelecimento
  WHERE f.obstrucao_confirmada),
por_periodo AS (
  SELECT estabelecimento, trecho, periodo,
         row_number() OVER (PARTITION BY estabelecimento, trecho ORDER BY count(*) DESC, periodo) AS rn
  FROM base GROUP BY estabelecimento, trecho, periodo)
SELECT b.estabelecimento, b.trecho,
       count(*)                                AS obstrucoes_confirmadas,
       count(DISTINCT b.sk_data_deteccao)      AS dias_com_obstrucao,
       max(p.periodo)                          AS periodo_mais_comum,
       round(sum(b.min_ate_resolucao) / 60, 1) AS horas_bloqueado
FROM base b
JOIN por_periodo p ON p.estabelecimento = b.estabelecimento AND p.trecho = b.trecho AND p.rn = 1
GROUP BY b.estabelecimento, b.trecho
ORDER BY obstrucoes_confirmadas DESC, horas_bloqueado DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Consulta 5 · IMPACTO: "O AccessFlow está reduzindo o tempo em que a passagem fica bloqueada?"
-- MAGIC Evolução semanal das horas de bloqueio, da % resolvida em até 30 min e da % verificada pelo robô,
-- MAGIC com a cobertura do serviço (unidades monitoradas × unidades do município no CNES e população do IBGE).

-- COMMAND ----------

WITH semanal AS (
  SELECT date_trunc('WEEK', f.detectado_em)                                   AS semana,
         count(*)                                                             AS obstrucoes,
         round(sum(f.min_ate_resolucao) / 60, 1)                              AS horas_bloqueio,
         round(avg(f.min_ate_resolucao), 1)                                   AS media_min_resolucao,
         round(100 * avg(CASE WHEN f.resolvida_em_ate_30min THEN 1 ELSE 0 END), 1) AS pct_resolvida_ate_30min,
         round(100 * avg(CASE WHEN f.verificado_em IS NOT NULL THEN 1 ELSE 0 END), 1) AS pct_verificada
  FROM workspace.accessflow.ouro_fato_ocorrencia f
  WHERE f.obstrucao_confirmada
  GROUP BY 1),
cobertura AS (
  SELECT count_if(monitorado_accessflow)                     AS unidades_monitoradas,
         count(*)                                            AS unidades_cnes_municipio,
         max(populacao_municipio)                            AS populacao_municipio
  FROM workspace.accessflow.ouro_dim_estabelecimento)
SELECT date_format(s.semana, 'dd/MM/yyyy') AS semana_inicio, s.obstrucoes, s.horas_bloqueio,
       s.media_min_resolucao,
       round(s.media_min_resolucao - first_value(s.media_min_resolucao) OVER (ORDER BY s.semana), 1)
                                           AS variacao_vs_1a_semana_min,
       s.pct_resolvida_ate_30min, s.pct_verificada,
       c.unidades_monitoradas, c.unidades_cnes_municipio,
       round(100 * c.unidades_monitoradas / c.unidades_cnes_municipio, 2) AS pct_cobertura,
       c.populacao_municipio
FROM semanal s CROSS JOIN cobertura c
ORDER BY s.semana;
