# Arquitetura de dados do AccessFlow

```mermaid
flowchart LR
  subgraph Fontes
    CNES[API Dados Abertos Saúde<br/>CNES · estabelecimentos]
    IBGE[API IBGE<br/>população por município]
    AF[API AccessFlow<br/>ocorrências + inspeções<br/>visão · robô · RPA]
  end
  subgraph Airflow["Airflow 3 · DAG gs_accessflow_coleta (diário 06:00)"]
    T1[coletar_cnes]
    T2[coletar_ibge]
    T3[coletar_accessflow]
    C[conferir_chegada] --> N[notificar_resumo]
  end
  CNES --> T1
  IBGE --> T2
  AF --> T3
  T1 & T2 & T3 --> C
  C --> P[(pasta chegada/<br/>1 arquivo por coleta)]
  P -- upload manual em lotes --> V[(Volume Databricks<br/>/Volumes/workspace/accessflow/chegada)]
  subgraph Databricks["Databricks Free Edition · Delta Lake"]
    B[Bronze<br/>Auto Loader incremental]
    S[Prata<br/>tipos · regras · MERGE]
    G[Ouro<br/>fatos + dimensões · LGPD]
    VW[views ouro_vw_mvp_*]
  end
  V --> B --> S --> G --> VW
  VW -- SQL Warehouse --> API[Backend AccessFlow]
  API --> P1[Página pública do cidadão]
  API --> P2[Painel do estabelecimento]
```

## Modelo dimensional (Ouro)

```mermaid
erDiagram
  ouro_fato_ocorrencia }o--|| ouro_dim_estabelecimento : sk_estabelecimento
  ouro_fato_ocorrencia }o--|| ouro_dim_trecho : sk_trecho
  ouro_fato_ocorrencia }o--|| ouro_dim_tipo_obstaculo : sk_tipo_obstaculo
  ouro_fato_ocorrencia }o--o| ouro_dim_responsavel : sk_responsavel
  ouro_fato_ocorrencia }o--|| ouro_dim_data : sk_data_deteccao
  ouro_fato_inspecao }o--|| ouro_dim_trecho : sk_trecho
  ouro_fato_inspecao }o--|| ouro_dim_estabelecimento : sk_estabelecimento
  ouro_fato_inspecao }o--|| ouro_dim_data : sk_data
  ouro_fato_ocorrencia {
    string id_ocorrencia PK
    string fonte
    string status
    double confianca
    double min_ate_resolucao
    boolean resolvida_em_ate_30min
  }
  ouro_dim_estabelecimento {
    bigint sk_estabelecimento PK
    string cnes
    string tipo_unidade
    bigint populacao_municipio
  }
  ouro_dim_responsavel {
    string sk_responsavel PK "SHA-256 + sal"
    string nome_mascarado
    string email_mascarado
  }
```

## Decisões

| Decisão | Por quê |
|---|---|
| NDJSON na chegada (CNES, AccessFlow) | 1 registro por linha: Auto Loader lê em paralelo e um registro ruim não invalida o arquivo |
| IBGE com schema explícito (`MAP`) | a chave do JSON é o ano; um ano novo não quebra a Bronze |
| Bronze tudo string + `_rescued_data` | Bronze não decide tipo; mudança na fonte não derruba a ingestão |
| MERGE com versão (`atualizado_em`) | a mesma ocorrência chega várias vezes; nunca regride para um status antigo |
| Quarentena em vez de descarte | dá para auditar e corrigir o que foi barrado |
| Ouro recriada a partir da Prata | determinística: reexecutar dá o mesmo resultado |
| Hash com sal via DataFrame | o sal não aparece no SQL nem no histórico de consultas |
| Views `ouro_vw_mvp_*` como contrato | o front não depende do layout interno das tabelas |
