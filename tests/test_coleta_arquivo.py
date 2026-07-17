import json
from io import BytesIO

import pytest

from rastro_publico.coleta.arquivo import baixar_arquivo


class Resposta(BytesIO):
    status = 200
    headers = {"Last-Modified": "Fri, 17 Jul 2026 00:00:00 GMT"}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None


def test_baixa_arquivo_atomico_com_manifesto(tmp_path) -> None:
    destino = tmp_path / "compras.csv"

    manifesto = baixar_arquivo(
        url="https://dados.example/compras.csv",
        destino=destino,
        sistema_origem="comprasgov",
        dataset_origem="VW_FT_PNCP_COMPRA",
        run_id="run-1",
        abrir=lambda *_args, **_kwargs: Resposta(b"id,nome\n1,teste\n"),
        tamanho_bloco=4,
    )

    assert destino.read_bytes() == b"id,nome\n1,teste\n"
    assert not destino.with_suffix(".csv.part").exists()
    assert manifesto == destino.with_suffix(".csv.manifest.json")
    documento = json.loads(manifesto.read_text(encoding="utf-8"))
    assert documento["sistema_origem"] == "comprasgov"
    assert documento["canal_entrega"] == "repositorio_csv"
    assert documento["dataset_origem"] == "VW_FT_PNCP_COMPRA"
    assert documento["tamanho_bytes"] == 16
    assert documento["hash_arquivo"] == "c5011ec4d1ed9e0980d4d111e3d85fb036b001e6bb068b56adbc546dd313bf10"
    assert documento["data_publicacao_arquivo"] == "Fri, 17 Jul 2026 00:00:00 GMT"


def test_falha_nao_substitui_arquivo_existente(tmp_path) -> None:
    destino = tmp_path / "compras.csv"
    destino.write_bytes(b"anterior")

    class RespostaComFalha(Resposta):
        def read(self, _size=-1):
            raise OSError("falha simulada")

    with pytest.raises(OSError, match="falha simulada"):
        baixar_arquivo(
            url="https://dados.example/compras.csv",
            destino=destino,
            sistema_origem="comprasgov",
            dataset_origem="VW_FT_PNCP_COMPRA",
            run_id="run-2",
            abrir=lambda *_args, **_kwargs: RespostaComFalha(),
        )

    assert destino.read_bytes() == b"anterior"
    assert not destino.with_suffix(".csv.part").exists()
