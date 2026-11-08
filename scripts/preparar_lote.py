"""Separa os arquivos da chegada/ em um lote para upload manual no Volume do Databricks.

O Databricks Free Edition não conversa com o Airflow local, então os arquivos sobem
pela interface. Este script copia as coletas de um intervalo de datas para
lotes/lote_NN/<subpasta>/, mantendo a mesma estrutura de pastas do Volume, e
escreve um manifesto (o que entrou no lote) para colocar no relatório.

Uso:
  python scripts/preparar_lote.py 1 2026-11-01 2026-11-09   # lote 1
  python scripts/preparar_lote.py 2 2026-11-10 2026-11-14   # lote 2 (dias diferentes)
"""
import re
import shutil
import sys
from datetime import date
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
CHEGADA = RAIZ / "chegada"


def main(numero: str, inicio: str, fim: str) -> None:
    ini, f = date.fromisoformat(inicio), date.fromisoformat(fim)
    destino = RAIZ / "lotes" / f"lote_{int(numero):02d}"
    shutil.rmtree(destino, ignore_errors=True)
    manifesto = []
    for arq in sorted(CHEGADA.rglob("*.json")):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", arq.name)
        if m and ini <= date.fromisoformat(m.group(1)) <= f:
            alvo = destino / arq.relative_to(CHEGADA)
            alvo.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(arq, alvo)
            manifesto.append(f"{arq.relative_to(CHEGADA)}\t{arq.stat().st_size} bytes")
    (destino / "MANIFESTO.txt").write_text("\n".join(manifesto) + "\n", encoding="utf-8")
    print(f"{len(manifesto)} arquivos em {destino}")
    print("Suba cada subpasta para /Volumes/workspace/accessflow/chegada/<mesma subpasta>/")


if __name__ == "__main__":
    main(*sys.argv[1:4])
