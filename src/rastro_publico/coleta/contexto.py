from __future__ import annotations

import csv
import json
import re
import sys
import zipfile
from argparse import ArgumentParser
from pathlib import Path

from rastro_publico.coleta.arquivo import manifestar_arquivo_existente


CAMPOS_EMPRESAS = (
    "cnpj_basico",
    "razao_social",
    "natureza_juridica",
    "qualificacao_responsavel",
    "capital_social",
    "porte_empresa",
    "ente_federativo_responsavel",
)
CAMPOS_SOCIOS_MINIMIZADOS = (
    "registro_origem_id",
    "cnpj_basico",
    "tipo_socio",
    "qualificacao_socio",
    "data_entrada_sociedade",
    "pais",
    "qualificacao_representante",
    "faixa_etaria",
)
CAMPOS_SANCOES_MINIMIZADOS = (
    "cadastro",
    "codigo_sancao",
    "cnpj_sancionado",
    "categoria_sancao",
    "valor_multa",
    "data_inicio_sancao",
    "data_final_sancao",
    "data_publicacao",
    "abrangencia_sancao",
    "orgao_sancionador",
    "uf_orgao_sancionador",
    "esfera_orgao_sancionador",
    "data_origem_informacao",
)


def filtrar_cnpj_fornecedores(
    *,
    diretorio: Path,
    cnpjs: set[str],
    destino_empresas: Path,
    destino_socios: Path,
) -> dict[str, int]:
    basicos = {
        digitos[:8]
        for valor in cnpjs
        if len(digitos := re.sub(r"\D", "", valor)) == 14
    }
    destino_empresas.parent.mkdir(parents=True, exist_ok=True)
    destino_socios.parent.mkdir(parents=True, exist_ok=True)
    temporario_empresas = destino_empresas.with_suffix(".csv.part")
    temporario_socios = destino_socios.with_suffix(".csv.part")
    contagens = {"cnpjs_basicos_alvo": len(basicos), "empresas": 0, "socios": 0}

    try:
        with temporario_empresas.open("w", encoding="utf-8", newline="") as saida:
            escritor = csv.writer(saida)
            escritor.writerow(CAMPOS_EMPRESAS)
            for linha in _linhas_zip(diretorio.glob("Empresas*.zip")):
                if linha and linha[0] in basicos:
                    escritor.writerow((linha + [""] * len(CAMPOS_EMPRESAS))[:7])
                    contagens["empresas"] += 1

        with temporario_socios.open("w", encoding="utf-8", newline="") as saida:
            escritor = csv.writer(saida)
            escritor.writerow(CAMPOS_SOCIOS_MINIMIZADOS)
            for indice, linha in enumerate(
                _linhas_zip(diretorio.glob("Socios*.zip")), start=1
            ):
                if linha and linha[0] in basicos:
                    completa = linha + [""] * 11
                    escritor.writerow(
                        (
                            str(indice),
                            completa[0],
                            completa[1],
                            completa[4],
                            completa[5],
                            completa[6],
                            completa[9],
                            completa[10],
                        )
                    )
                    contagens["socios"] += 1

        temporario_empresas.replace(destino_empresas)
        temporario_socios.replace(destino_socios)
    except Exception:
        temporario_empresas.unlink(missing_ok=True)
        temporario_socios.unlink(missing_ok=True)
        raise
    return contagens


def filtrar_sancoes_fornecedores(
    *, arquivo: Path, cadastro: str, cnpjs: set[str], destino: Path
) -> int:
    alvos = {
        digitos
        for valor in cnpjs
        if len(digitos := re.sub(r"\D", "", valor)) == 14
    }
    cadastro = cadastro.upper()
    if cadastro not in {"CEIS", "CNEP"}:
        raise ValueError("cadastro deve ser CEIS ou CNEP")
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_suffix(".csv.part")
    contagem = 0
    deslocamento = 1 if cadastro == "CNEP" else 0
    try:
        with temporario.open("w", encoding="utf-8", newline="") as saida:
            escritor = csv.writer(saida)
            escritor.writerow(CAMPOS_SANCOES_MINIMIZADOS)
            linhas = _linhas_zip([arquivo])
            next(linhas, None)
            for linha in linhas:
                completa = linha + [""] * 25
                cnpj = re.sub(r"\D", "", completa[3])
                if cnpj not in alvos:
                    continue
                escritor.writerow(
                    (
                        cadastro,
                        completa[1],
                        cnpj,
                        completa[9],
                        completa[10] if cadastro == "CNEP" else "",
                        completa[10 + deslocamento],
                        completa[11 + deslocamento],
                        completa[12 + deslocamento],
                        completa[16 + deslocamento],
                        completa[17 + deslocamento],
                        completa[18 + deslocamento],
                        completa[19 + deslocamento],
                        completa[21 + deslocamento],
                    )
                )
                contagem += 1
        temporario.replace(destino)
    except Exception:
        temporario.unlink(missing_ok=True)
        raise
    return contagem


def _linhas_zip(arquivos):
    for caminho in sorted(arquivos):
        with zipfile.ZipFile(caminho) as compactado:
            nomes = [nome for nome in compactado.namelist() if not nome.endswith("/")]
            if len(nomes) != 1:
                raise ValueError(f"esperado um CSV em {caminho.name}")
            with compactado.open(nomes[0]) as binario:
                texto = (linha.decode("latin-1") for linha in binario)
                yield from csv.reader(texto, delimiter=";")


def main() -> None:
    parser = ArgumentParser(
        description="Filtra a base CNPJ/QSA pelos fornecedores recebidos em JSON."
    )
    parser.add_argument("--diretorio", type=Path, required=True)
    parser.add_argument("--destino", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--competencia", required=True)
    parser.add_argument("--ceis", type=Path)
    parser.add_argument("--cnep", type=Path)
    parser.add_argument("--somente-sancoes", action="store_true")
    args = parser.parse_args()
    documento = json.load(sys.stdin)
    cnpjs = {
        linha[0]
        for linha in documento.get("result", {}).get("data_array", [])
        if linha and linha[0]
    }
    resumo = {"cnpjs_basicos_alvo": len({cnpj[:8] for cnpj in cnpjs})}
    if not args.somente_sancoes:
        empresas = args.destino / "empresas_fornecedores.csv"
        socios = args.destino / "socios_fornecedores_minimizado.csv"
        resumo = filtrar_cnpj_fornecedores(
            diretorio=args.diretorio,
            cnpjs=cnpjs,
            destino_empresas=empresas,
            destino_socios=socios,
        )
        for arquivo, dataset in (
            (empresas, "cnpj_empresas_fornecedores"),
            (socios, "cnpj_qsa_fornecedores_minimizado"),
        ):
            manifestar_arquivo_existente(
                arquivo=arquivo,
                url="https://arquivos.receitafederal.gov.br/",
                sistema_origem="receita_federal",
                dataset_origem=dataset,
                run_id=args.run_id,
                data_publicacao_arquivo=args.competencia,
                canal_entrega="webdav_recorte_local",
            )
    sancoes = {}
    for cadastro, origem in (("CEIS", args.ceis), ("CNEP", args.cnep)):
        if not origem:
            continue
        saida = args.destino / f"{cadastro.lower()}_fornecedores_minimizado.csv"
        sancoes[cadastro.lower()] = filtrar_sancoes_fornecedores(
            arquivo=origem,
            cadastro=cadastro,
            cnpjs=cnpjs,
            destino=saida,
        )
        manifestar_arquivo_existente(
            arquivo=saida,
            url="https://portaldatransparencia.gov.br/download-de-dados",
            sistema_origem="cgu_portal_transparencia",
            dataset_origem=f"{cadastro.lower()}_fornecedores_minimizado",
            run_id=args.run_id,
            data_publicacao_arquivo=args.competencia,
            canal_entrega="download_oficial_recorte_local",
        )
    resumo["sancoes"] = sancoes
    print(json.dumps(resumo, ensure_ascii=False))


if __name__ == "__main__":
    main()
