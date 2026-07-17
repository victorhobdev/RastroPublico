import pytest
from pyspark.sql import SparkSession

from rastro_publico.coleta.persistencia import filtrar_novas_linhas


@pytest.fixture(scope="module")
def spark():
    sessao = SparkSession.builder.master("local[2]").appName("rastro-publico-test").getOrCreate()
    yield sessao
    sessao.stop()


def test_anti_join_impede_reprocessar_mesma_chave(spark) -> None:
    entrada = spark.createDataFrame([("a", 1), ("b", 2)], ["observacao_id", "valor"])
    existente = spark.createDataFrame([("a",)], ["observacao_id"])

    resultado = filtrar_novas_linhas(entrada, existente, "observacao_id").collect()

    assert [(linha.observacao_id, linha.valor) for linha in resultado] == [("b", 2)]
