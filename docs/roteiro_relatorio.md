# Roteiro do relatório em PDF (ordem exigida pelo professor)

> Prints legíveis, **com legenda**, e nesta ordem. Faltar nome completo ou RM de alguém = −0,5.

| # | Seção | O que colocar | De onde tirar |
|---|---|---|---|
| 01 | Integrantes | Nome completo + RM de todos | — |
| 02 | Produto e perguntas | "AccessFlow identifica obstáculos em passagens acessíveis de unidades de saúde, aciona o responsável e verifica a resolução." + as 5 perguntas abaixo | README |
| 03 | Diagrama do fluxo | Fonte → Airflow → chegada/ → Volume → Bronze → Prata → Ouro → API → telas | `docs/arquitetura.md` (Mermaid) |
| 04 | Tabela de fontes | Origem, formato, frequência | README › Fontes |
| 05 | Requisitos 01–09 | 1 print + 2–3 linhas por requisito, na ordem | tabela abaixo |
| 06 | Airflow | Visão **Graph** e **Grid** com execuções verdes | UI do Airflow |
| 07 | Contagens Bronze | Antes/depois do 2º lote e da 2ª execução (sem duplicar) | última célula do `02_bronze` |
| 08 | 5 consultas | SQL + resultado + interpretação (2–3 linhas cada) | `05_consultas_mvp` |
| 09 | Conclusão + link | O que funcionou, limitações (CNES/IBGE são foto do dia; ocorrências simuladas até a API subir), próximos passos, link do GitHub | — |

## Perguntas que o MVP responde (seção 02)

1. A passagem acessível desta unidade está livre agora? (cidadão)
2. O que está pendente no meu estabelecimento e há quanto tempo? (responsável)
3. Quais obstáculos demoram mais para sair e qual fonte detecta melhor? (operação)
4. Onde e quando os bloqueios se repetem? (prevenção)
5. **O AccessFlow reduz o tempo em que a passagem fica bloqueada?** (impacto)

## Prints da seção 05

| Req. | Print | Explicação curta |
|---|---|---|
| 01 | Tabela de fontes + 1 resposta da API do CNES no navegador | CNES e IBGE são externas/abertas; AccessFlow é a fonte do sistema |
| 02 | Código do DAG (schedule) + pasta `chegada/` com arquivos datados | Agendado 06:00, 1 arquivo por coleta com data no nome |
| 03 | Graph mostrando 3 tasks paralelas + `conferir_chegada`; tela Admin › Variables | Chaves só em Variables (token mascarado) |
| 04 | `lotes/lote_01/MANIFESTO.txt` e `lote_02` + tela do Volume com os arquivos | Lotes de dias diferentes, upload pela interface |
| 05 | Tabela `controle_contagens` (camada bronze) e `cloud_files_state` | 2º lote: só novos; 3ª execução: +0 |
| 06 | `prata_regras_qualidade` + `prata_quarentena` + trecho do MERGE | 5 regras nas ocorrências com total barrado |
| 07 | Lista de tabelas `ouro_*` (fatos e dimensões) + diagrama estrela | 2 fatos, 5 dimensões |
| 08 | `ouro_dim_responsavel` (hash/máscara) + tela do MVP | Qual view alimenta qual tela |
| 09 | Página do repositório no GitHub | README com integrantes e passo a passo |

## Checklist antes de enviar

- [ ] Repositório **público** e sem `chegada/`, tokens ou `.env`
- [ ] `amostras/` preenchida com `scripts/gerar_amostras.py`
- [ ] Nomes completos + RM no README e no PDF
- [ ] Interpretação escrita para as 5 consultas (não só o print)
- [ ] Fontes adaptadas ao AccessFlow (não usar o exemplo de clima do enunciado)
- [ ] Enviado por 1 integrante no Teams até 19/11, 23h55
