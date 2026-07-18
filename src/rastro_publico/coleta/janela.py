from __future__ import annotations

import json
import csv
import hashlib
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from rastro_publico.coleta.arquivo import baixar_arquivo


def _hash_arquivo(caminho: Path) -> str:
    digest = hashlib.sha256()
    with caminho.open("rb") as arquivo:
        for bloco in iter(lambda: arquivo.read(1024 * 1024), b""):
            digest.update(bloco)
    return digest.hexdigest()


def fragmentar_csv_logico(
    origem: Path,
    destino: Path,
    *,
    max_bytes: int = 256 * 1024 * 1024,
) -> list[Path]:
    """Fragmenta CSV preservando registros com quebras de linha internas."""
    destino.mkdir(parents=True, exist_ok=True)
    manifesto_origem = json.loads(
        origem.with_suffix(".csv.manifest.json").read_text(encoding="utf-8")
    )
    csv.field_size_limit(2**31 - 1)
    partes: list[Path] = []
    with origem.open(encoding="utf-8-sig", newline="") as entrada:
        leitor = csv.reader(entrada)
        cabecalho = next(leitor)
        saida = None
        escritor = None
        linhas_parte = 0
        try:
            for linha in leitor:
                if saida is None:
                    caminho = destino / (
                        f"{origem.stem}.part-{len(partes):05d}.csv"
                    )
                    saida = caminho.open("w", encoding="utf-8", newline="")
                    escritor = csv.writer(saida, lineterminator="\n")
                    escritor.writerow(cabecalho)
                    partes.append(caminho)
                    linhas_parte = 0
                escritor.writerow(linha)
                linhas_parte += 1
                if saida.tell() >= max_bytes:
                    saida.close()
                    saida = None
                    escritor = None
                    _gravar_manifesto_parte(
                        partes[-1], manifesto_origem, origem.name, linhas_parte
                    )
            if saida is not None:
                saida.close()
                saida = None
                _gravar_manifesto_parte(
                    partes[-1], manifesto_origem, origem.name, linhas_parte
                )
        finally:
            if saida is not None:
                saida.close()
    return partes


def _gravar_manifesto_parte(
    caminho: Path,
    manifesto_origem: dict,
    arquivo_origem: str,
    linhas: int,
) -> None:
    manifesto = {
        **manifesto_origem,
        "arquivo": caminho.name,
        "arquivo_origem": arquivo_origem,
        "linhas_dados": linhas,
        "tamanho_bytes": caminho.stat().st_size,
        "hash_arquivo": _hash_arquivo(caminho),
        "canal_entrega": "repositorio_csv_anual_fragmentado",
    }
    caminho.with_suffix(".csv.manifest.json").write_text(
        json.dumps(manifesto, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def definir_arquivos_anuais(anos: list[int]) -> dict[str, dict[str, str]]:
    arquivos = {}
    comprasgov = {
        "compras": "VW_FT_PNCP_COMPRA",
        "itens": "VW_FT_PNCP_COMPRA_ITEM",
        "resultados": "VW_DM_PNCP_ITEM_RESULTADO",
    }
    contratos = {
        "contratos": "contratos",
        "contratos_itens": "itens",
        "historicos": "historicos",
    }
    for ano in sorted(set(anos)):
        for nome, dataset in comprasgov.items():
            arquivo = f"comprasGOV-anual-{dataset}-{ano}.csv"
            arquivos[f"{nome}_{ano}"] = {
                "url": (
                    "https://repositorio.dados.gov.br/seges/comprasgov/anual/"
                    f"{ano}/{arquivo}"
                ),
                "arquivo": arquivo,
                "sistema": "comprasgov",
                "dataset": dataset,
                "ano": str(ano),
            }
        for nome, dataset in contratos.items():
            arquivo = f"comprasnet-contratos-anual-{dataset}-{ano}.csv"
            arquivos[f"{nome}_{ano}"] = {
                "url": (
                    "https://repositorio.dados.gov.br/seges/"
                    f"comprasnet_contratos/anual/{ano}/{arquivo}"
                ),
                "arquivo": arquivo,
                "sistema": "comprasnet_contratos",
                "dataset": dataset,
                "ano": str(ano),
            }
    return arquivos


def main() -> None:
    parser = ArgumentParser(description="Baixa snapshots anuais para a janela móvel.")
    parser.add_argument("--destino", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--anos", nargs="+", type=int, required=True)
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    fontes = definir_arquivos_anuais(args.anos)

    def baixar(nome_fonte):
        nome, fonte = nome_fonte
        destino = args.destino / fonte["arquivo"]
        manifesto = destino.with_suffix(".csv.manifest.json")
        if destino.exists() and manifesto.exists():
            return nome, "existente", str(manifesto)
        caminho = baixar_arquivo(
            url=fonte["url"],
            destino=destino,
            sistema_origem=fonte["sistema"],
            dataset_origem=fonte["dataset"],
            run_id=args.run_id,
            canal_entrega="repositorio_csv_anual",
            minimo_bytes=100,
            timeout=1800,
        )
        return nome, "baixado", str(caminho)

    resultado = {}
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futuros = {executor.submit(baixar, fonte): fonte[0] for fonte in fontes.items()}
        for futuro in as_completed(futuros):
            nome, estado, manifesto = futuro.result()
            resultado[nome] = {"estado": estado, "manifesto": manifesto}
            print(json.dumps({nome: resultado[nome]}, ensure_ascii=False), flush=True)
    print(json.dumps(resultado, ensure_ascii=False))


if __name__ == "__main__":
    main()
