# AccessFlow · Big Data Architecture & Data Integration

**GS 2026.2 · CitizenTech · Engenharia de Software 4º ano (4ES)**

> AccessFlow identifica obstáculos em passagens acessíveis de clínicas e unidades de saúde,
> aciona o responsável e verifica se o problema foi resolvido. Este repositório é o
> **componente Dados**: o pipeline que coleta, trata (Bronze → Prata → Ouro) e entrega ao MVP
> o que o cidadão e o estabelecimento veem.

## Integrantes

| Nome completo | RM |
|---|---|
| Juliana Villalpando Maita | RM99224 |
| João Victor Dos Santos Morais | RM550453 |
| Luana Cabezaolias | RM99320 |
| Lucca Vilaça Okubo | RM551538 |
| Pedro Henrique Pontes Farath | RM98608 |

## Perguntas que o MVP responde

1. A passagem acessível desta unidade está livre agora? → **página pública**
2. O que está pendente no meu estabelecimento e há quanto tempo? → **painel**
3. Quais obstáculos demoram mais e qual fonte (visão, robô, manual) detecta melhor?
4. Onde e quando os bloqueios se repetem?
5. **Impacto:** o AccessFlow está reduzindo o tempo em que a passagem fica bloqueada?

## Arquitetura

```
CNES (API aberta) ─┐
IBGE (API aberta) ─┼─> Airflow (DAG diário) ─> chegada/ ─(upload em lotes)─> Volume Databricks
AccessFlow (API)  ─┘        3 coletas em paralelo                                │
                            + conferência + notificação                          ▼
                     Bronze (Auto Loader) ─> Prata (regras + MERGE) ─> Ouro (fatos/dimensões, LGPD)
                                                                                  │
                                       views ouro_vw_mvp_* ─> API ─> página do cidadão / painel
```

Diagramas completos (fluxo e modelo estrela) em [`docs/arquitetura.md`](docs/arquitetura.md).

## Fontes

| Fonte | Tipo | Origem | Formato | Frequência |
|---|---|---|---|---|
| CNES — estabelecimentos de saúde | **Externa, dados abertos** | `apidadosabertos.saude.gov.br/cnes/estabelecimentos` (Ministério da Saúde) | JSON paginado → NDJSON | diária |
| População por município | **Externa, dados abertos** | `servicodados.ibge.gov.br/api/v3/agregados/6579` (IBGE, estimativa) | JSON aninhado | diária (muda 1x/ano) |
| Ocorrências e inspeções | Sistema AccessFlow | API do produto (`docs/contrato_api.md`) ou **simulador documentado** | NDJSON | diária (incremental por `atualizado_em`) |

**Sobre o simulador:** até a API do AccessFlow estar publicada, `dags/accessflow_lib/simulador.py`
gera os registros no mesmo contrato, de forma determinística, usando CNES **reais** coletados do
Ministério da Saúde. Inclui anomalias propositais (confiança inválida, campos nulos, CNES
inexistente, cronologia quebrada, duplicatas) para demonstrar as regras da Prata. Dados pessoais
são 100% fictícios (`@exemplo.org.br`). Ao preencher a Variable `accessflow_api_url`, o DAG passa a
ler da API real sem mudar nada no lakehouse.

## Estrutura

```
dags/                       DAG do Airflow + biblioteca de coleta e simulador
databricks/
  00_config.py              nomes, MERGE, regras de qualidade, contagens (usado via %run)
  01_setup.py               schema, Volume e pastas (rodar 1x)
  02_bronze.py              Auto Loader incremental + evidências de contagem
  03_prata.py               tipos, nulos, 5 regras com contagem, MERGE pela chave
  04_ouro.py                fatos, dimensões, LGPD e views do MVP
  05_consultas_mvp.sql      as 5 consultas (1 de impacto)
amostras/                   um pouco de cada fonte (gerado por scripts/gerar_amostras.py)
mvp/consulta_ouro.py        como a API do MVP lê a Ouro
scripts/                    preparar lotes de upload e amostras
tests/                      teste local da lógica Spark (sem Databricks)
docs/                       arquitetura, contrato da API, roteiro do relatório
```

## Como reproduzir

### 1. Airflow (coleta)

```bash
docker compose up -d                       # Airflow 3 em http://localhost:8080
docker compose exec airflow airflow variables import /opt/airflow/config_accessflow/variables.example.json
```

Em **Admin › Variables** (nada disso fica no código):

| Variable | Valor |
|---|---|
| `cnes_codigo_municipio` | `355030` (São Paulo, 6 dígitos) |
| `ibge_localidades` | `N6[N3[35]]` (todos os municípios de SP) |
| `accessflow_cnes_piloto` | após a 1ª coleta, fixe os CNES do piloto, ex. `["<cnes1>","<cnes2>"]` (JSON) |
| `accessflow_api_url` / `accessflow_api_token` | vazio = simulador; preenchido = API real |
| `accessflow_webhook_notificacao` | webhook do n8n (RPA) / Teams / Discord (opcional) |

Ative o DAG `gs_accessflow_coleta`. Com `catchup=True` ele roda os dias desde 01/11, gravando
`chegada/<fonte>/<fonte>_AAAA-MM-DD.json`.

### 2. Lotes para o Databricks

```bash
python scripts/preparar_lote.py 1 2026-11-01 2026-11-09
python scripts/preparar_lote.py 2 2026-11-10 2026-11-14
```

No Databricks: **Catalog › workspace › accessflow › Volumes › chegada**, entre em cada subpasta
e use **Upload to this volume** com os arquivos da subpasta correspondente do lote.

### 3. Lakehouse

Importe a pasta `databricks/` (Workspace › Import, ou Git folder apontando para este repositório) e rode:

1. `01_setup` (uma vez) e crie o segredo do hash: `databricks secrets create-scope accessflow` +
   `databricks secrets put-secret accessflow salt_hash` (ou preencha o widget do `04_ouro`)
2. Suba o **lote 1** → `02_bronze` → `03_prata` → `04_ouro`
3. Suba o **lote 2** → `02_bronze` (só os novos entram) → rode `02_bronze` de novo (nada duplica)
4. `03_prata` → `04_ouro` → `05_consultas_mvp`

### 4. Teste local opcional (sem Databricks, sem internet)

```bash
pip install pyspark
python -I tests/gerar_chegada_teste.py /tmp/chegada 2026-10-20 2026-11-08   # CNES/IBGE fictícios
python -I tests/teste_local_spark.py /tmp/chegada
```

## Qualidade (Prata)

| Tabela | Regra | Tratamento |
|---|---|---|
| ocorrências | `obrigatorios_ausentes` — id, CNES, trecho ou detecção nulos | barrado |
| ocorrências | `confianca_fora_0_1` — confiança da visão fora de [0,1] | barrado |
| ocorrências | `cnes_nao_cadastrado` — CNES inexistente no cadastro | barrado |
| ocorrências | `cronologia_invalida` — resolução antes da detecção | barrado |
| ocorrências | `dominio_invalido` — status/fonte fora da lista | barrado |
| inspeções | obrigatórios, resultado inválido, CNES não cadastrado | barrado |
| estabelecimentos | CNES fora do padrão de 7 dígitos | barrado |
| estabelecimentos | coordenada 0,0 ou fora do Brasil | vira nulo |
| municípios | população nula/não numérica | barrado |
| todas | duplicata exata na origem; várias versões da mesma chave | removida / fica a mais recente |

Contagem por regra em `prata_regras_qualidade`; registros barrados em `prata_quarentena`.

## LGPD

* Nenhum dado do cidadão é coletado: a página pública é anônima.
* O único dado pessoal é o do **responsável** (funcionário do estabelecimento).
  Bronze: bruto, acesso restrito. Prata: telefone descartado. **Ouro: SHA-256 com sal secreto +
  nome/e-mail mascarados** (`J*** S***`, `j***@dominio`).
* Sal, tokens e webhooks só em Databricks Secrets / Airflow Variables. `chegada/` e `lotes/` estão no `.gitignore`.

## Contrato com o MVP

| View da Ouro | Tela |
|---|---|
| `ouro_vw_mvp_status_publico` | Página pública: status do trecho, última inspeção, orientação |
| `ouro_vw_mvp_painel_estabelecimento` | Cards do painel: trechos, obstruções abertas, tempo pendente |
| `ouro_vw_mvp_ocorrencias_abertas` | Lista de pendências (responsável mascarado) |

Exemplo de consumo: [`mvp/consulta_ouro.py`](mvp/consulta_ouro.py).

## ODS

ODS 3 (saúde e bem-estar: acesso físico a unidades de saúde), ODS 10 (redução das desigualdades:
pessoas com deficiência e mobilidade reduzida) e ODS 11 (cidades inclusivas e acessíveis).
