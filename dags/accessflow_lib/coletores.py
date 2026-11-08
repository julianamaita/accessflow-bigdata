"""Funções de coleta chamadas pelas tasks do DAG.

Cada coleta grava UM arquivo por execução em chegada/<fonte>/<fonte>_<AAAA-MM-DD>.json.
Os arquivos são o dado bruto da fonte (sem transformação): quem trata é a Prata.
Rodar a mesma data de novo sobrescreve o mesmo arquivo -> coleta idempotente.
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path

import requests

from accessflow_lib import simulador

log = logging.getLogger(__name__)

URL_CNES = "https://apidadosabertos.saude.gov.br/cnes/estabelecimentos"
URL_IBGE = ("https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/-1"
            "/variaveis/9324?localidades={localidades}")
TIMEOUT = 60
CABECALHOS = {"User-Agent": "accessflow-gs-fiap/1.0"}

# tipos CNES de interesse para o piloto (clínicas, UBS, policlínicas, hospitais, PA)
TIPOS_PILOTO = {"1", "2", "4", "5", "7", "36", "39", "62", "73"}


def _gravar_ndjson(registros: list[dict], destino: Path) -> int:
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for r in registros:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    tmp.replace(destino)  # escrita atômica: o Volume nunca recebe arquivo pela metade
    return len(registros)


# ------------------------------------------------------------------ CNES (externa)
def coletar_cnes(chegada: Path, ds: str, codigo_municipio: str, max_paginas: int = 25,
                 limite: int = 20) -> dict:
    """Estabelecimentos de saúde do município (API de Dados Abertos do Ministério da Saúde)."""
    registros, offset = [], 0
    for _ in range(max_paginas):
        r = requests.get(URL_CNES, params={"codigo_municipio": codigo_municipio,
                                           "limit": limite, "offset": offset},
                         headers=CABECALHOS, timeout=TIMEOUT)
        r.raise_for_status()
        pagina = r.json().get("estabelecimentos", [])
        registros.extend(pagina)
        if len(pagina) < limite:
            break
        offset += limite
    if not registros:
        raise ValueError(f"CNES não retornou estabelecimentos para {codigo_municipio}")
    qtd = _gravar_ndjson(registros, chegada / "cnes" / f"cnes_{ds}.json")
    log.info("CNES: %s estabelecimentos gravados", qtd)
    return {"fonte": "cnes", "registros": qtd}


# ------------------------------------------------------------------ IBGE (externa)
SNAPSHOT_IBGE = "/opt/airflow/config_accessflow/ibge_populacao_snapshot.json"


def coletar_ibge(chegada: Path, ds: str, localidades: str, snapshot: str = SNAPSHOT_IBGE) -> dict:
    """População estimada por município (agregado 6579, variável 9324). Resposta bruta.

    A API do IBGE recusa conexões de fora do Brasil (ex.: GitHub Codespaces). Nesse caso
    usamos o snapshot baixado manualmente da MESMA URL (config/ibge_populacao_snapshot.json).
    A população muda 1x por ano, então o snapshot não perde validade no período da GS.
    """
    try:
        r = requests.get(URL_IBGE.format(localidades=localidades), headers=CABECALHOS, timeout=TIMEOUT)
        r.raise_for_status()
        dados, modo = r.json(), "api"
    except requests.RequestException as exc:
        log.warning("IBGE indisponível (%s); usando snapshot %s", exc, snapshot)
        dados, modo = json.loads(Path(snapshot).read_text(encoding="utf-8")), "snapshot"
    log.info("IBGE modo=%s", modo)
    destino = chegada / "ibge" / f"ibge_populacao_{ds}.json"
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(json.dumps(dados, ensure_ascii=False), encoding="utf-8")
    qtd = sum(len(res["series"]) for v in dados for res in v["resultados"])
    log.info("IBGE: %s municípios", qtd)
    return {"fonte": "ibge", "registros": qtd}


# ------------------------------------------------------------------ AccessFlow (sistema)
def _estabelecimentos_piloto(chegada: Path, cnes_piloto: list[str] | None, qtd: int = 8):
    """Quais estabelecimentos o AccessFlow monitora (usado só no modo simulado)."""
    if cnes_piloto:
        return [simulador.Estabelecimento(str(c)) for c in cnes_piloto]
    arquivos = sorted((chegada / "cnes").glob("cnes_*.json"))
    if not arquivos:
        # 1ª execução: a task do CNES roda em paralelo e ainda não gravou. O retry resolve.
        raise FileNotFoundError("Nenhum arquivo do CNES ainda; aguardando retry da task.")
    candidatos = []
    with arquivos[-1].open(encoding="utf-8") as f:
        for linha in f:
            e = json.loads(linha)
            if str(e.get("codigo_tipo_unidade")) in TIPOS_PILOTO:
                candidatos.append(simulador.Estabelecimento(str(e["codigo_cnes"]), e.get("nome_fantasia", "")))
    candidatos.sort(key=lambda e: e.cnes)
    return candidatos[:qtd]


def coletar_accessflow(chegada: Path, ds: str, api_url: str | None, api_token: str | None,
                       cnes_piloto: list[str] | None) -> dict:
    """Ocorrências e inspeções do sistema AccessFlow (API real ou simulador)."""
    dia_ref = date.fromisoformat(ds) - timedelta(days=1)  # coleta de hoje traz o dia anterior completo
    if api_url:
        headers = {**CABECALHOS, "Authorization": f"Bearer {api_token}"} if api_token else CABECALHOS
        params = {"atualizado_desde": f"{dia_ref}T00:00:00-03:00", "atualizado_ate": f"{ds}T00:00:00-03:00"}
        ocorrencias = _paginar(f"{api_url.rstrip('/')}/ocorrencias", params, headers)
        inspecoes = _paginar(f"{api_url.rstrip('/')}/inspecoes", params, headers)
        modo = "api"
    else:
        ests = _estabelecimentos_piloto(chegada, cnes_piloto)
        ocorrencias = simulador.gerar_ocorrencias(dia_ref, ests)
        inspecoes = simulador.gerar_inspecoes(dia_ref, ests)
        modo = "simulado"
    q_oc = _gravar_ndjson(ocorrencias, chegada / "accessflow_ocorrencias" / f"ocorrencias_{ds}.json")
    q_in = _gravar_ndjson(inspecoes, chegada / "accessflow_inspecoes" / f"inspecoes_{ds}.json")
    log.info("AccessFlow (%s): %s ocorrências, %s inspeções", modo, q_oc, q_in)
    return {"fonte": "accessflow", "modo": modo, "ocorrencias": q_oc, "inspecoes": q_in}


def _paginar(url: str, params: dict, headers: dict) -> list[dict]:
    itens, pagina = [], 1
    while True:
        r = requests.get(url, params={**params, "pagina": pagina}, headers=headers, timeout=TIMEOUT)
        r.raise_for_status()
        corpo = r.json()
        itens.extend(corpo.get("itens", []))
        if not corpo.get("proxima_pagina"):
            return itens
        pagina += 1


# ------------------------------------------------------------------ conferência
ESPERADOS = {
    "cnes": "cnes/cnes_{ds}.json",
    "ibge": "ibge/ibge_populacao_{ds}.json",
    "ocorrencias": "accessflow_ocorrencias/ocorrencias_{ds}.json",
    "inspecoes": "accessflow_inspecoes/inspecoes_{ds}.json",
}


def conferir_chegada(chegada: Path, ds: str) -> dict:
    """Confere se cada arquivo do dia chegou, não está vazio e é JSON válido."""
    resumo, faltando = {}, []
    for fonte, padrao in ESPERADOS.items():
        arq = chegada / padrao.format(ds=ds)
        # dia sem ocorrência é válido (arquivo vazio); fonte externa vazia não é
        if not arq.exists() or (arq.stat().st_size == 0 and fonte in ("cnes", "ibge")):
            faltando.append(str(arq))
            continue
        texto = arq.read_text(encoding="utf-8")
        if arq.name.startswith("ibge"):
            json.loads(texto)
            resumo[fonte] = 1
        else:
            linhas = [l for l in texto.splitlines() if l.strip()]
            for l in linhas:
                json.loads(l)
            resumo[fonte] = len(linhas)
    if faltando:
        raise FileNotFoundError(f"Arquivos ausentes ou vazios: {faltando}")
    log.info("Chegada %s conferida: %s", ds, resumo)
    return resumo
