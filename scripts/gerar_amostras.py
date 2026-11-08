"""Copia um pouco de cada fonte (coleta mais recente) para amostras/, como pede a entrega.

Remove dados de contato pessoais das amostras do AccessFlow por precaução
(no modo simulado já são fictícios). Rode depois da primeira coleta real.

Uso:  python scripts/gerar_amostras.py [linhas=15]
"""
import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
CHEGADA, AMOSTRAS = RAIZ / "chegada", RAIZ / "amostras"
PASTAS = ["cnes", "ibge", "accessflow_ocorrencias", "accessflow_inspecoes"]


def anonimizar(reg: dict) -> dict:
    if isinstance(reg.get("responsavel"), dict):
        reg["responsavel"] = {"nome": "<removido>", "email": "<removido>", "telefone": "<removido>"}
    for campo in ("numero_telefone_estabelecimento", "endereco_email_estabelecimento"):
        if campo in reg:
            reg[campo] = "<removido>"
    return reg


def main(linhas: int = 15) -> None:
    AMOSTRAS.mkdir(exist_ok=True)
    for pasta in PASTAS:
        arquivos = sorted((CHEGADA / pasta).glob("*.json"))
        if not arquivos:
            print(f"sem arquivos em chegada/{pasta}")
            continue
        origem = arquivos[-1]
        texto = origem.read_text(encoding="utf-8")
        if pasta == "ibge":
            dados = json.loads(texto)
            for v in dados:
                for r in v["resultados"]:
                    r["series"] = r["series"][:linhas]
            saida = json.dumps(dados, ensure_ascii=False, indent=2)
        else:
            regs = [anonimizar(json.loads(l)) for l in texto.splitlines()[:linhas] if l.strip()]
            saida = "\n".join(json.dumps(r, ensure_ascii=False) for r in regs) + "\n"
        (AMOSTRAS / f"amostra_{origem.name}").write_text(saida, encoding="utf-8")
        print(f"amostras/amostra_{origem.name}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 15)
