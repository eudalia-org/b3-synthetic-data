#!/usr/bin/env python3
"""
roda_ordem.py

Chama ordem_insercao_oracle.py para descobrir a ordem de inserção no Oracle.
Lê o specs do mesmo JSON que o synthesizer usa (DATAGEN_SPECS_URI / arquivo local).

Uso:
    python roda_ordem.py                      # usa ./specs_full.json
    python roda_ordem.py caminho/specs.json   # usa outro caminho
"""

import json
import sys

# importa do arquivo entregue (precisa estar na mesma pasta ou no PYTHONPATH)
from ordem_insercao_oracle import ordem_insercao, imprime_relatorio


def carrega_specs(caminho: str) -> dict:
    with open(caminho, "r", encoding="utf-8") as f:
        specs = json.load(f)
    if not isinstance(specs, dict) or not specs:
        raise ValueError(f"specs em `{caminho}` deve ser um objeto JSON não vazio.")
    return specs


def main() -> None:
    caminho = sys.argv[1] if len(sys.argv) > 1 else "specs_full.json"
    specs = carrega_specs(caminho)

    # Ordena TODAS as tabelas do specs (as 47 sintetizadas vão pro banco juntas).
    # Para ordenar só as 15 do CDB, passe apenas=QUINZE:
    #   from ordem_insercao_oracle import ordem_insercao
    #   QUINZE = ["CARTEIRA_COMITENTE", "CARTEIRA_PARTICIPANTE", "ESPECIFICACAO",
    #             "ESPECIFICACAO_COMITENTE", "INSTRUMENTO_FINANCEIRO", "LANCAMENTO",
    #             "OPERACAO", "TITULO", "EVENTO", "CONDICAO_IF", "CREDITO",
    #             "DADO_OPERACAO", "DEPOSITO_AUTOMATICO_IF", "JUROS_FLUTUANTE", "RESGATE"]
    #   rel = ordem_insercao(specs, apenas=QUINZE)
    rel = ordem_insercao(specs)

    imprime_relatorio(rel)

    # A ordem pura, como lista Python — útil para iterar a carga no Oracle:
    print("\nLista para uso programático:")
    print(rel["ordem"])


if __name__ == "__main__":
    main()



import json
from ordem_insercao_oracle import ordem_insercao
specs = json.load(open("specs_full.json", encoding="utf-8"))
ordem = ordem_insercao(specs)["ordem"]
