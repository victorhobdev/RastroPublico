import os
import sys

import pytest
from pyspark.sql import SparkSession

from rastro_publico.transformacoes.gold import (
    calcular_concentracao_fornecedores,
    calcular_qualidade_cobertura,
)


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    sessao = SparkSession.builder.master("local[2]").appName("gold-test").getOrCreate()
    yield sessao
    sessao.stop()


def entradas(spark):
    itens = spark.createDataFrame(
        [
            ("c1", "i1", "computador_notebook", "UN", 1.0, 100.0),
            ("c1", "i2", "computador_notebook", "UN", 1.0, 200.0),
            ("c1", "i3", "computador_notebook", "UN", 1.0, 300.0),
            ("c2", "i4", "monitor", None, 1.0, None),
            ("c3", "i5", "incerto", "UN", 1.0, 50.0),
        ],
        [
            "contratacao_id",
            "item_id",
            "categoria_tecnologia",
            "unidade_medida",
            "quantidade",
            "valor_unitario_estimado",
        ],
    )
    resultados = spark.createDataFrame(
        [
            ("r1", "i1", "f1", 60.0, False),
            ("r2", "i2", "f1", 30.0, False),
            ("r3", "i3", "f2", 10.0, False),
            ("r4", "i4", "f3", 20.0, True),
        ],
        [
            "resultado_id",
            "item_id",
            "fornecedor_id",
            "valor_total_homologado",
            "cancelado",
        ],
    )
    contratacoes = spark.createDataFrame(
        [
            ("c1", "2026-07-01", "6", "Pregao"),
            ("c2", "2026-07-02", "8", "Dispensa"),
            ("c3", None, None, None),
        ],
        ["contratacao_id", "publicado_em", "modalidade_id", "modalidade"],
    )
    vinculos = spark.createDataFrame(
        [("c1", "o1"), ("c2", "o2")],
        ["contratacao_id", "orgao_id"],
    )
    qualidade = spark.createDataFrame(
        [
            ("run-1", "registros_em_quarentena", 2, 100, 2.0, "alerta", "ALERT", "2026-07-17"),
            ("run-2", "registros_em_quarentena", 3, 120, 2.5, "alerta", "ALERT", "2026-07-18"),
        ],
        [
            "run_id",
            "regra",
            "observados",
            "total",
            "percentual",
            "severidade",
            "status",
            "registrado_em",
        ],
    )
    return itens, resultados, contratacoes, vinculos, qualidade


def test_qualidade_publica_coberturas_e_ultimo_resultado_operacional(spark) -> None:
    gold = calcular_qualidade_cobertura(*entradas(spark))

    notebook = gold.where("categoria_tecnologia = 'computador_notebook'").first()
    operacional = gold.where("metrica = 'registros_em_quarentena'").first()

    assert notebook.total_itens == 3
    assert notebook.itens_com_resultado == 3
    assert notebook.cobertura_fornecedor == 1.0
    assert notebook.cobertura_categoria == 1.0
    assert operacional.observados == 3
    assert operacional.total == 120


def test_concentracao_reconcilia_top1_top3_hhi_e_estado(spark) -> None:
    itens, resultados, contratacoes, vinculos, _ = entradas(spark)

    gold = calcular_concentracao_fornecedores(
        itens,
        resultados,
        contratacoes,
        vinculos,
        minimo_fornecedores=2,
        minimo_resultados=3,
    ).first()

    assert gold.valor_total_homologado == pytest.approx(100.0)
    assert gold.fornecedores_distintos == 2
    assert gold.resultados_elegiveis == 3
    assert gold.top_1 == pytest.approx(0.9)
    assert gold.top_3 == pytest.approx(1.0)
    assert gold.hhi == pytest.approx(0.82)
    assert gold.status_publicacao == "publicada"


def test_concentracao_nao_publica_grupo_sem_populacao_minima(spark) -> None:
    itens, resultados, contratacoes, vinculos, _ = entradas(spark)

    gold = calcular_concentracao_fornecedores(
        itens,
        resultados,
        contratacoes,
        vinculos,
        minimo_fornecedores=3,
        minimo_resultados=3,
    ).first()

    assert gold.status_publicacao == "nao_publicavel"
    assert gold.limitacao == "fornecedores_insuficientes"
