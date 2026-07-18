from __future__ import annotations

import json
from argparse import ArgumentParser
from pathlib import Path

from rastro_publico.coleta.arquivo import baixar_arquivo


def definir_fontes(
    *,
    data_referencia: str,
    periodo_inicial_ipca: str,
    periodo_final_ipca: str,
) -> dict[str, dict[str, str]]:
    return {
        "municipios_ibge": {
            "url": "https://servicodados.ibge.gov.br/api/v1/localidades/municipios",
            "sistema_origem": "ibge",
            "dataset_origem": "localidades_municipios",
            "arquivo": "municipios_ibge.json",
        },
        "ipca_indice": {
            "url": (
                "https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/2266/p/"
                f"{periodo_inicial_ipca}-{periodo_final_ipca}?formato=json"
            ),
            "sistema_origem": "ibge_sidra",
            "dataset_origem": "ipca_numero_indice_1737_2266",
            "arquivo": "ipca_indice.json",
        },
        "ceis": {
            "url": (
                "https://portaldatransparencia.gov.br/download-de-dados/ceis/"
                f"{data_referencia}"
            ),
            "sistema_origem": "cgu_portal_transparencia",
            "dataset_origem": "ceis",
            "arquivo": "ceis.zip",
        },
        "cnep": {
            "url": (
                "https://portaldatransparencia.gov.br/download-de-dados/cnep/"
                f"{data_referencia}"
            ),
            "sistema_origem": "cgu_portal_transparencia",
            "dataset_origem": "cnep",
            "arquivo": "cnep.zip",
        },
    }


def main() -> None:
    parser = ArgumentParser(description="Baixa fontes contextuais oficiais.")
    parser.add_argument("--destino", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--data-referencia", required=True)
    parser.add_argument("--ipca-inicio", required=True)
    parser.add_argument("--ipca-fim", required=True)
    args = parser.parse_args()
    fontes = definir_fontes(
        data_referencia=args.data_referencia,
        periodo_inicial_ipca=args.ipca_inicio,
        periodo_final_ipca=args.ipca_fim,
    )
    manifestos = {}
    for nome, fonte in fontes.items():
        manifesto = baixar_arquivo(
            url=fonte["url"],
            destino=args.destino / fonte["arquivo"],
            sistema_origem=fonte["sistema_origem"],
            dataset_origem=fonte["dataset_origem"],
            run_id=args.run_id,
            canal_entrega="api_http",
            minimo_bytes=10,
            timeout=300,
        )
        manifestos[nome] = str(manifesto)
    print(json.dumps(manifestos, ensure_ascii=False))


if __name__ == "__main__":
    main()
