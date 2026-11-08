"""Notificações do DAG (falha de task e resumo da execução).

O destino é um webhook configurado na Variable `accessflow_webhook_notificacao`
(ex.: o fluxo de n8n da disciplina de RPA, ou um canal do Teams/Slack/Discord).
Sem a Variable, a mensagem só vai para o log da task, e o DAG não quebra.
"""
from __future__ import annotations

import logging

import requests

log = logging.getLogger(__name__)


def _webhook() -> str | None:
    from airflow.sdk import Variable
    return Variable.get("accessflow_webhook_notificacao", default=None)


def enviar(texto: str, dados: dict | None = None) -> None:
    url = _webhook()
    log.info("NOTIFICAÇÃO: %s | %s", texto, dados)
    if not url:
        return
    try:
        requests.post(url, json={"text": texto, "dados": dados or {}}, timeout=15).raise_for_status()
    except requests.RequestException as exc:  # notificação nunca derruba o pipeline
        log.warning("Falha ao enviar notificação: %s", exc)


def ao_falhar(context) -> None:
    """on_failure_callback: avisa qual task falhou, em qual data e o link do log."""
    ti = context["ti"]
    enviar(f"[AccessFlow] Falha na task {ti.task_id} (coleta {context['ds']})",
           {"dag": ti.dag_id, "task": ti.task_id, "tentativa": ti.try_number,
            "erro": str(context.get("exception"))[:500]})
