# Contrato de dados AccessFlow → Big Data

Formato único que **API, visão computacional, robô e RPA** usam para registrar ocorrências
e inspeções. O DAG lê exatamente isto (da API real ou do simulador).

## Endpoints lidos pelo Airflow

| Método | Rota | Parâmetros | Resposta |
|---|---|---|---|
| GET | `/ocorrencias` | `atualizado_desde`, `atualizado_ate` (ISO 8601), `pagina` | `{"itens": [Ocorrencia], "proxima_pagina": 2 \| null}` |
| GET | `/inspecoes` | idem | `{"itens": [Inspecao], "proxima_pagina": ...}` |

Autenticação: `Authorization: Bearer <token>` (token na Variable `accessflow_api_token`).

A extração é **incremental por `atualizado_em`**: se uma ocorrência mudou de status no dia,
ela volta inteira, no estado atual. A Prata resolve as versões com MERGE pela chave.

## Ocorrencia

```json
{
  "id_ocorrencia": "OC-2077485-20261110-03",
  "cnes": "2077485",
  "trecho": {"id": "2077485-T1", "nome": "Entrada principal", "tipo": "entrada"},
  "fonte": "visao",                    // visao | robo | manual
  "tipo_obstaculo": "caixa",           // caixa | carrinho | cadeira | maca | lixeira | outro
  "confianca": 0.91,                   // só visao (YOLO), entre 0 e 1
  "distancia_cm": null,                // só robo (ultrassônico)
  "status": "resolvida",               // aberta | atribuida | resolvida | verificada | descartada
  "confirmado_por_humano": true,       // pessoa no circuito (IA responsável)
  "detectado_em":  "2026-11-10T08:30:00-03:00",
  "atribuido_em":  "2026-11-10T08:40:00-03:00",
  "resolvido_em":  "2026-11-10T08:54:00-03:00",
  "verificado_em": null,
  "atualizado_em": "2026-11-10T08:54:00-03:00",
  "responsavel": {"nome": "...", "email": "...", "telefone": "..."}
}
```

Quem preenche cada campo:

| Campo | Origem |
|---|---|
| `fonte`, `tipo_obstaculo`, `confianca` | Computer Vision (YOLO) |
| `distancia_cm` | Maker Lab (robô com sensor ultrassônico) |
| `confirmado_por_humano`, `atribuido_em`, `responsavel` | Painel do estabelecimento / RPA |
| `resolvido_em` | Responsável marca no painel (RPA acompanha) |
| `verificado_em` | Próxima ronda do robô que encontra o trecho livre |

## Inspecao

```json
{
  "id_inspecao": "IN-2077485-T1-20261110-08",
  "cnes": "2077485",
  "trecho": {"id": "2077485-T1", "nome": "Entrada principal", "tipo": "entrada"},
  "executado_por": "robo-01",
  "inspecionado_em": "2026-11-10T08:03:00-03:00",
  "resultado": "livre",                // livre | obstruido
  "distancia_minima_cm": 310,
  "id_ocorrencia": null                // preenchido quando obstruido
}
```

## LGPD

* O único dado pessoal é o `responsavel` (funcionário do estabelecimento). Nenhum dado do
  cidadão é coletado — a página pública é anônima.
* Bronze guarda o bruto (acesso restrito à equipe de dados); Prata descarta o telefone;
  Ouro só tem hash SHA-256 com sal + nome/e-mail mascarados.
