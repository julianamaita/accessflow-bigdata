"""Como o backend do AccessFlow consome a Ouro (requisito 08).

A API do MVP lê as views ouro_vw_mvp_* pelo SQL Warehouse serverless do
Databricks e devolve JSON para as telas. Credenciais em variáveis de
ambiente (nunca no código):
  DATABRICKS_HOST       ex.: dbc-xxxx.cloud.databricks.com
  DATABRICKS_HTTP_PATH  ex.: /sql/1.0/warehouses/xxxx
  DATABRICKS_TOKEN      token pessoal (no deploy: secret do GitHub/Cloud)

Rotas sugeridas para o backend (FastAPI/Express):
  GET /publico/estabelecimentos/{cnes}/acesso  -> status_publico(cnes)   [página do cidadão]
  GET /painel/{cnes}/resumo                    -> painel(cnes)           [cards do painel]
  GET /painel/{cnes}/pendencias                -> pendencias(cnes)       [lista do painel]

Teste:  python mvp/consulta_ouro.py <cnes>
"""
import json
import os
import sys

from databricks import sql

VIEWS = "workspace.accessflow"


def _consultar(query: str, params: dict) -> list[dict]:
    with sql.connect(server_hostname=os.environ["DATABRICKS_HOST"],
                     http_path=os.environ["DATABRICKS_HTTP_PATH"],
                     access_token=os.environ["DATABRICKS_TOKEN"]) as conn, conn.cursor() as cur:
        cur.execute(query, params)  # parâmetro nomeado: sem SQL injection
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, linha)) for linha in cur.fetchall()]


def status_publico(cnes: str) -> list[dict]:
    """Página pública: só dados do estabelecimento/trecho, nenhum dado pessoal."""
    return _consultar(f"""SELECT estabelecimento, trecho, status_acesso, bloqueado_desde,
                                 ultima_inspecao, orientacao
                          FROM {VIEWS}.ouro_vw_mvp_status_publico WHERE cnes = :cnes
                          ORDER BY status_acesso DESC, trecho""", {"cnes": cnes})


def painel(cnes: str) -> list[dict]:
    return _consultar(f"SELECT * FROM {VIEWS}.ouro_vw_mvp_painel_estabelecimento WHERE cnes = :cnes",
                      {"cnes": cnes})


def pendencias(cnes: str) -> list[dict]:
    """Painel interno: responsável aparece mascarado (J*** S***), nunca em claro."""
    return _consultar(f"""SELECT trecho, obstaculo, criticidade, fonte, status, minutos_pendente,
                                 responsavel, contato_responsavel
                          FROM {VIEWS}.ouro_vw_mvp_ocorrencias_abertas WHERE cnes = :cnes
                          ORDER BY minutos_pendente DESC""", {"cnes": cnes})


if __name__ == "__main__":
    cnes = sys.argv[1]
    print(json.dumps({"publico": status_publico(cnes), "painel": painel(cnes),
                      "pendencias": pendencias(cnes)}, default=str, ensure_ascii=False, indent=2))
