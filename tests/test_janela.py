import csv
import json

from rastro_publico.coleta.janela import (
    definir_arquivos_anuais,
    fragmentar_csv_logico,
)


def test_define_arquivos_anuais_para_janela_que_cruza_anos() -> None:
    arquivos = definir_arquivos_anuais([2025, 2026])

    assert len(arquivos) == 12
    assert arquivos["compras_2025"]["url"].endswith(
        "/comprasgov/anual/2025/comprasGOV-anual-VW_FT_PNCP_COMPRA-2025.csv"
    )
    assert arquivos["historicos_2026"]["url"].endswith(
        "/comprasnet_contratos/anual/2026/"
        "comprasnet-contratos-anual-historicos-2026.csv"
    )
    assert arquivos["itens_2026"]["dataset"] == "VW_FT_PNCP_COMPRA_ITEM"


def test_fragmenta_csv_sem_quebrar_registro_multilinha(tmp_path) -> None:
    origem = tmp_path / "comprasGOV-anual-DADOS-2026.csv"
    with origem.open("w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.writer(arquivo)
        escritor.writerow(["id", "descricao"])
        escritor.writerow(["1", "linha 1\nlinha 2"])
        escritor.writerow(["2", "texto simples"])
    origem.with_suffix(".csv.manifest.json").write_text(
        json.dumps(
            {
                "run_id": "teste",
                "sistema_origem": "comprasgov",
                "dataset_origem": "DADOS",
                "url_origem": "https://exemplo.test/dados.csv",
                "coletado_em_utc": "2026-07-18T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    partes = fragmentar_csv_logico(origem, tmp_path / "partes", max_bytes=25)

    assert len(partes) == 2
    linhas = []
    for parte in partes:
        with parte.open(encoding="utf-8", newline="") as arquivo:
            linhas.extend(list(csv.DictReader(arquivo)))
        manifesto = json.loads(
            parte.with_suffix(".csv.manifest.json").read_text(encoding="utf-8")
        )
        assert manifesto["hash_arquivo"]
        assert manifesto["arquivo_origem"] == origem.name
    assert linhas == [
        {"id": "1", "descricao": "linha 1\nlinha 2"},
        {"id": "2", "descricao": "texto simples"},
    ]
