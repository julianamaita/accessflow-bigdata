"""DAG de coleta do AccessFlow (Big Data · GS 2026.2).

Fluxo:  inicio >> [coletar_cnes, coletar_ibge, coletar_accessflow] >> conferir_chegada >> notificar_resumo

* Agendado diariamente; catchup=True gera as coletas de dias anteriores
  (lotes de dias diferentes para o requisito 04).
* As três coletas rodam em paralelo (requisito 03).
* conferir_chegada valida os arquivos do dia na pasta chegada/ (requisito 03).
* Toda configuração e credencial vem de Airflow Variables (nunca do código).
* Falhas disparam on_failure_callback; ao final, um resumo é notificado.
"""
from __future__ import annotations

import json
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

sys.path.insert(0, str(Path(__file__).resolve().parent))  # garante import da lib ao lado do DAG
from accessflow_lib import coletores, notificacao  # noqa: E402


def _chegada() -> Path:
    return Path(Variable.get("accessflow_pasta_chegada", default="/opt/airflow/chegada"))


def task_cnes(ds, **_):
    return coletores.coletar_cnes(
        _chegada(), ds,
        codigo_municipio=Variable.get("cnes_codigo_municipio", default="355030"),
        max_paginas=int(Variable.get("cnes_max_paginas", default="25")),
    )


def task_ibge(ds, **_):
    return coletores.coletar_ibge(
        _chegada(), ds, localidades=Variable.get("ibge_localidades", default="N6[N3[35]]"))


def task_accessflow(ds, **_):
    piloto = Variable.get("accessflow_cnes_piloto", default="")
    return coletores.coletar_accessflow(
        _chegada(), ds,
        api_url=Variable.get("accessflow_api_url", default="") or None,
        api_token=Variable.get("accessflow_api_token", default="") or None,  # mascarada no log
        cnes_piloto=json.loads(piloto) if piloto else None,
    )


def task_conferir(ds, **_):
    return coletores.conferir_chegada(_chegada(), ds)


def task_resumo(ds, ti, **_):
    resumo = ti.xcom_pull(task_ids="conferir_chegada") or {}
    status = "OK" if resumo else "INCOMPLETA"
    notificacao.enviar(f"[AccessFlow] Coleta {ds} {status}", resumo)


padrao = {
    "owner": "accessflow-dados",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notificacao.ao_falhar,
}

with DAG(
    dag_id="gs_accessflow_coleta",
    description="Coleta diária: CNES + IBGE + ocorrências/inspeções AccessFlow -> chegada/",
    schedule="0 6 * * *",                       # todo dia às 06:00 (America/Sao_Paulo)
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    catchup=True,
    max_active_runs=1,
    default_args=padrao,
    tags=["accessflow", "gs", "bigdata"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    cnes = PythonOperator(task_id="coletar_cnes", python_callable=task_cnes)
    ibge = PythonOperator(task_id="coletar_ibge", python_callable=task_ibge)
    accessflow = PythonOperator(task_id="coletar_accessflow", python_callable=task_accessflow)
    conferir = PythonOperator(task_id="conferir_chegada", python_callable=task_conferir, retries=0)
    resumo = PythonOperator(task_id="notificar_resumo", python_callable=task_resumo,
                            trigger_rule="all_done", retries=0)

    inicio >> [cnes, ibge, accessflow] >> conferir >> resumo
