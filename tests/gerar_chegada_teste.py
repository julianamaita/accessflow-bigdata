"""Gera uma pasta chegada/ de TESTE, sem Airflow e sem internet.

CNES e IBGE aqui são FICTÍCIOS (fixtures com o mesmo formato das APIs), só
para testar a lógica localmente. Para a entrega, use os arquivos que o DAG
baixou das APIs reais.

Uso:  python -I tests/gerar_chegada_teste.py <pasta_saida> 2026-11-01 2026-11-07
"""
import json
import sys
from datetime import date, timedelta
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "dags"))
from accessflow_lib import coletores  # noqa: E402

FIX = RAIZ / "tests" / "fixtures"


def main(saida: str, inicio: str, fim: str) -> None:
    chegada = Path(saida)
    d, d_fim = date.fromisoformat(inicio), date.fromisoformat(fim)
    cnes = [json.loads(l) for l in (FIX / "cnes_ficticio.ndjson").read_text(encoding="utf-8").splitlines()]
    ibge = (FIX / "ibge_ficticio.json").read_text(encoding="utf-8")
    while d <= d_fim:
        ds = d.isoformat()
        coletores._gravar_ndjson(cnes, chegada / "cnes" / f"cnes_{ds}.json")
        (chegada / "ibge").mkdir(parents=True, exist_ok=True)
        (chegada / "ibge" / f"ibge_populacao_{ds}.json").write_text(ibge, encoding="utf-8")
        print(ds, coletores.coletar_accessflow(chegada, ds, None, None, None),
              coletores.conferir_chegada(chegada, ds))
        d += timedelta(days=1)


if __name__ == "__main__":
    main(*sys.argv[1:4])
