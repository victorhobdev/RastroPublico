import os
import sys

import pytest
from pyspark.sql import SparkSession

from rastro_publico.coleta.arquivo_bronze import arquivo_ja_carregado, preparar_csv_bronze
from rastro_publico.coleta.persistencia import filtrar_novas_linhas


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    sessao = SparkSession.builder.master("local[2]").appName("rastro-publico-test").getOrCreate()
    yield sessao
    sessao.stop()


def test_anti_join_impede_reprocessar_mesma_chave(spark) -> None:
    entrada = spark.createDataFrame([("a", 1), ("b", 2)], ["observacao_id", "valor"])
    existente = spark.createDataFrame([("a",)], ["observacao_id"])

    resultado = filtrar_novas_linhas(entrada, existente, "observacao_id").collect()

    assert [(linha.observacao_id, linha.valor) for linha in resultado] == [("b", 2)]


def test_prepara_csv_bronze_com_proveniencia(spark, tmp_path) -> None:
    caminho = tmp_path / "compras.csv"
    caminho.write_text("id_compra,objeto\n1,Notebook\n2,Monitor\n", encoding="utf-8")

    resultado = preparar_csv_bronze(
        spark,
        str(caminho),
        {
            "source_file_id": "hash-1",
            "run_id": "run-1",
            "sistema_origem": "comprasgov",
            "dataset_origem": "VW_FT_PNCP_COMPRA",
            "coletado_em_utc": "2026-07-17T23:35:22+00:00",
        },
    )

    assert resultado.count() == 2
    assert set(resultado.columns) == {
        "id_compra",
        "objeto",
        "_source_file_id",
        "_run_id",
        "_sistema_origem",
        "_dataset_origem",
        "_coletado_em_utc",
    }
    assert {linha._source_file_id for linha in resultado.collect()} == {"hash-1"}


def test_detecta_arquivo_ja_carregado(spark, monkeypatch) -> None:
    existente = spark.createDataFrame([("hash-1",)], ["_source_file_id"])
    monkeypatch.setattr(spark.catalog, "tableExists", lambda _tabela: True)
    monkeypatch.setattr(spark, "table", lambda _tabela: existente)

    assert arquivo_ja_carregado(spark, "bronze.contratacoes_raw", "hash-1")
    assert not arquivo_ja_carregado(spark, "bronze.contratacoes_raw", "hash-2")
