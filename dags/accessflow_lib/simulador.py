"""Simulador do sistema AccessFlow (ocorrências e inspeções).

Por que simular?
    O briefing permite "dados simulados com geração justificada e documentada".
    Enquanto a API do AccessFlow (frontend/backend da equipe) não estiver
    publicada, o DAG gera aqui os mesmos registros que a API devolveria,
    seguindo exatamente o contrato em docs/contrato_api.md. Quando a
    Variable `accessflow_api_url` for configurada, o DAG passa a ler da API
    real e este módulo deixa de ser usado.

Como a geração funciona
    * Determinística: a mesma data + mesmo CNES geram sempre os mesmos
      registros (random.Random com semente fixa). Rodar o DAG duas vezes
      para o mesmo dia produz o mesmo arquivo -> coleta idempotente.
    * Sem estado: cada ocorrência nasce num dia d0 e evolui
      (aberta -> atribuída -> resolvida -> verificada). O arquivo do dia D
      traz toda ocorrência que teve algum marco em D, no estado do fim de D.
      Por isso o mesmo id aparece em arquivos de dias diferentes, e a Prata
      precisa de MERGE pela chave para ficar com a versão mais recente.
    * Tendência: o tempo de resolução cai ao longo das semanas (efeito do
      acompanhamento automático), o que alimenta a consulta de impacto.
    * Anomalias propositais (documentadas): confiança fora de [0,1], data
      nula, CNES não cadastrado, resolução antes da detecção, linhas
      duplicadas e grafia inconsistente. Servem para demonstrar as regras de
      qualidade da Prata. Desligue com injetar_anomalias=False.
    * Dados pessoais 100% fictícios (domínio exemplo.org.br). Nenhum dado
      pessoal real é gerado nem versionado.
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

TZ_SP = timezone(timedelta(hours=-3))  # America/Sao_Paulo (sem horário de verão desde 2019)
INICIO_PILOTO = date(2026, 10, 1)       # referência para a tendência de melhoria

TRECHOS = [
    # (sufixo, nome, tipo, peso de ocorrência)
    ("T1", "Entrada principal", "entrada", 0.35),
    ("T2", "Corredor da recepção", "corredor", 0.30),
    ("T3", "Rampa de acesso", "rampa", 0.20),
    ("T4", "Banheiro acessível", "banheiro", 0.15),
]
TIPOS_OBSTACULO = [("caixa", 0.34), ("carrinho", 0.24), ("cadeira", 0.16),
                   ("maca", 0.10), ("lixeira", 0.09), ("outro", 0.07)]
FONTES = [("visao", 0.55), ("robo", 0.30), ("manual", 0.15)]
HORAS_RONDA = [8, 10, 12, 14, 16, 18]

# Pessoas fictícias (responsáveis por estabelecimento). Não são pessoas reais.
NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fábio", "Gabriela", "Heitor",
         "Isabela", "João", "Larissa", "Marcos", "Natália", "Otávio", "Paula", "Rafael"]
SOBRENOMES = ["Almeida", "Barbosa", "Cardoso", "Duarte", "Esteves", "Ferraz",
              "Gouveia", "Holanda", "Lacerda", "Moura", "Nogueira", "Pacheco"]


@dataclass(frozen=True)
class Estabelecimento:
    cnes: str
    nome: str = ""


def _escolher(rng: random.Random, opcoes):
    valores, pesos = zip(*[(o[0], o[-1]) for o in opcoes])
    return rng.choices(valores, weights=pesos, k=1)[0]


def _iso(dt: datetime | None) -> str | None:
    return dt.astimezone(TZ_SP).isoformat(timespec="seconds") if dt else None


def _responsaveis(cnes: str) -> list[dict]:
    rng = random.Random(f"resp-{cnes}")
    pessoas = []
    for _ in range(2):
        nome, sobrenome = rng.choice(NOMES), rng.choice(SOBRENOMES)
        usuario = f"{nome}.{sobrenome}".lower()
        usuario = (usuario.replace("á", "a").replace("é", "e").replace("í", "i")
                   .replace("ó", "o").replace("ã", "a").replace("ç", "c").replace("ú", "u"))
        pessoas.append({
            "nome": f"{nome} {sobrenome}",
            "email": f"{usuario}@exemplo.org.br",
            "telefone": f"+55 11 9{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}",
        })
    return pessoas


def _ocorrencias_do_dia(est: Estabelecimento, d0: date) -> list[dict]:
    """Gera, com seus marcos completos, as ocorrências que nasceram em d0."""
    rng = random.Random(f"oc-{est.cnes}-{d0.isoformat()}")
    semanas = max(0, (d0 - INICIO_PILOTO).days) / 7
    fator_melhoria = max(0.35, 1 - 0.12 * semanas)      # resolução fica mais rápida
    media_dia = 3.2 if d0.weekday() < 5 else 1.2           # menos movimento no fim de semana
    qtd = min(8, max(0, round(rng.gauss(media_dia, 1.3))))
    responsaveis = _responsaveis(est.cnes)
    ocorrencias = []
    for i in range(qtd):
        sufixo, nome_trecho, tipo_trecho, _ = _escolher_trecho(rng)
        fonte = _escolher(rng, FONTES)
        detectado = datetime.combine(d0, time(7, 0), TZ_SP) + timedelta(minutes=rng.randint(0, 12 * 60))
        falso_positivo = fonte != "manual" and rng.random() < 0.08
        atribuido = detectado + timedelta(minutes=rng.randint(1, 12))
        resolvido = verificado = None
        if not falso_positivo:
            minutos = rng.lognormvariate(math.log(55 * fator_melhoria), 0.7)
            if rng.random() < 0.08:  # obstáculo "esquecido": fica para o dia seguinte
                minutos = rng.randint(8 * 60, 26 * 60)
            resolvido = atribuido + timedelta(minutes=max(4, int(minutos)))
            if rng.random() < 0.93:  # algumas resoluções não são verificadas pelo robô
                verificado = resolvido + timedelta(minutes=rng.randint(5, 70))
        ocorrencias.append({
            "id_ocorrencia": f"OC-{est.cnes}-{d0:%Y%m%d}-{i:02d}",
            "cnes": est.cnes,
            "trecho": {"id": f"{est.cnes}-{sufixo}", "nome": nome_trecho, "tipo": tipo_trecho},
            "fonte": fonte,
            "tipo_obstaculo": _escolher(rng, TIPOS_OBSTACULO),
            "confianca": round(rng.uniform(0.55, 0.99), 3) if fonte == "visao" else None,
            "distancia_cm": rng.randint(15, 90) if fonte == "robo" else None,
            "_detectado": detectado,
            "_atribuido": atribuido,
            "_resolvido": resolvido,
            "_verificado": verificado,
            "_falso_positivo": falso_positivo,
            "responsavel": rng.choice(responsaveis),
        })
    return ocorrencias


def _escolher_trecho(rng: random.Random):
    return rng.choices(TRECHOS, weights=[t[3] for t in TRECHOS], k=1)[0]


def _estado_no_fim_do_dia(oc: dict, dia: date) -> dict | None:
    """Fotografia da ocorrência no fim de `dia`. None se nada mudou nesse dia."""
    fim = datetime.combine(dia + timedelta(days=1), time(0, 0), TZ_SP)
    marcos = {k: oc[k] for k in ("_detectado", "_atribuido", "_resolvido", "_verificado")}
    if not any(m and m.date() == dia for m in marcos.values()):
        return None
    vis = {k: (v if v and v < fim else None) for k, v in marcos.items()}
    if oc["_falso_positivo"]:
        status = "descartada" if vis["_atribuido"] else "aberta"
    elif vis["_verificado"]:
        status = "verificada"
    elif vis["_resolvido"]:
        status = "resolvida"
    elif vis["_atribuido"]:
        status = "atribuida"
    else:
        status = "aberta"
    atualizado = max(v for v in vis.values() if v)
    registro = {k: v for k, v in oc.items() if not k.startswith("_")}
    registro.update({
        "status": status,
        "confirmado_por_humano": (not oc["_falso_positivo"]) if vis["_atribuido"] else None,
        "detectado_em": _iso(vis["_detectado"]),
        "atribuido_em": _iso(vis["_atribuido"]),
        "resolvido_em": _iso(vis["_resolvido"]),
        "verificado_em": _iso(vis["_verificado"]),
        "atualizado_em": _iso(atualizado),
    })
    return registro


def gerar_ocorrencias(dia: date, estabelecimentos: list[Estabelecimento],
                      injetar_anomalias: bool = True) -> list[dict]:
    """Registros que a API devolveria para GET /ocorrencias?atualizado_em=dia."""
    registros = []
    for est in estabelecimentos:
        for atraso in range(0, 4):                      # ocorrências nascidas até 3 dias antes
            for oc in _ocorrencias_do_dia(est, dia - timedelta(days=atraso)):
                estado = _estado_no_fim_do_dia(oc, dia)
                if estado:
                    registros.append(estado)
    if injetar_anomalias:
        registros = _injetar_anomalias(registros, dia)
    return registros


def _injetar_anomalias(registros: list[dict], dia: date) -> list[dict]:
    rng = random.Random(f"anomalias-{dia.isoformat()}")
    saida = []
    for r in registros:
        r = dict(r)
        # só a 1ª versão (dia da detecção) recebe anomalia: as versões seguintes chegam
        # corretas e o MERGE as aplica, como num sistema real que corrige o dado depois
        primeira_versao = (r["detectado_em"] or "")[:10] == dia.isoformat()
        sorteio = rng.random() if primeira_versao else 1.0
        if sorteio < 0.04 and r["fonte"] == "visao":
            r["confianca"] = rng.choice([1.35, -0.2, 7.0])          # regra: confiança inválida
        elif sorteio < 0.06:
            r["detectado_em"] = None                                 # regra: obrigatórios
        elif sorteio < 0.08:
            r["cnes"] = "9999999"                                    # regra: integridade
        elif sorteio < 0.10 and r["resolvido_em"]:
            r["resolvido_em"] = "2020-01-01T00:00:00-03:00"          # regra: cronologia
        if rng.random() < 0.06:
            r["tipo_obstaculo"] = rng.choice([" Caixa", "CARRINHO", "Cadeira "])  # padronização
        saida.append(r)
        if rng.random() < 0.03:
            saida.append(dict(r))                                    # duplicata na origem
    return saida


def gerar_inspecoes(dia: date, estabelecimentos: list[Estabelecimento]) -> list[dict]:
    """Rondas do robô: uma leitura por trecho a cada 2h (registro de 'sem bloqueio' também)."""
    inspecoes = []
    for est in estabelecimentos:
        abertas = []
        for atraso in range(0, 4):
            for oc in _ocorrencias_do_dia(est, dia - timedelta(days=atraso)):
                if not oc["_falso_positivo"]:
                    abertas.append(oc)
        rng = random.Random(f"insp-{est.cnes}-{dia.isoformat()}")
        for sufixo, nome_trecho, tipo_trecho, _ in TRECHOS:
            for hora in HORAS_RONDA:
                if rng.random() < 0.05:  # robô offline / sem bateria: ronda perdida
                    continue
                momento = datetime.combine(dia, time(hora, rng.randint(0, 9)), TZ_SP)
                bloqueio = next((o for o in abertas
                                 if o["trecho"]["id"] == f"{est.cnes}-{sufixo}"
                                 and o["_detectado"] <= momento
                                 and (o["_resolvido"] is None or o["_resolvido"] > momento)), None)
                inspecoes.append({
                    "id_inspecao": f"IN-{est.cnes}-{sufixo}-{dia:%Y%m%d}-{hora:02d}",
                    "cnes": est.cnes,
                    "trecho": {"id": f"{est.cnes}-{sufixo}", "nome": nome_trecho, "tipo": tipo_trecho},
                    "executado_por": "robo-01",
                    "inspecionado_em": _iso(momento),
                    "resultado": "obstruido" if bloqueio else "livre",
                    "distancia_minima_cm": rng.randint(15, 90) if bloqueio else rng.randint(120, 400),
                    "id_ocorrencia": bloqueio["id_ocorrencia"] if bloqueio else None,
                })
    return inspecoes
