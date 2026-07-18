import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from rastro_publico.transformacoes.gold import (
    calcular_evolucao_contratual,
    calcular_cobertura_servicos,
    calcular_concentracao_fornecedores,
    calcular_presenca_fornecedores,
    calcular_qualidade_cobertura,
    calcular_recorrencia_orgao_fornecedor,
    calcular_rede_orgao_fornecedor,
    calcular_variacao_precos,
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
            (
                "run-1",
                "registros_em_quarentena",
                2,
                100,
                2.0,
                "alerta",
                "ALERT",
                "2026-07-17",
            ),
            (
                "run-2",
                "registros_em_quarentena",
                3,
                120,
                2.5,
                "alerta",
                "ALERT",
                "2026-07-18",
            ),
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
    assert float(gold.top_3) == pytest.approx(1.0)
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


def test_concentracao_limita_top3_apos_arredondamento_decimal(spark) -> None:
    itens = spark.createDataFrame(
        [
            ("c1", "i1", "computador_notebook"),
            ("c1", "i2", "computador_notebook"),
            ("c1", "i3", "computador_notebook"),
        ],
        ["contratacao_id", "item_id", "categoria_tecnologia"],
    )
    resultados = spark.createDataFrame(
        [
            ("r1", "i1", "f1", 1.0, False),
            ("r2", "i2", "f2", 1.0, False),
            ("r3", "i3", "f3", 4.0, False),
        ],
        [
            "resultado_id",
            "item_id",
            "fornecedor_id",
            "valor_total_homologado",
            "cancelado",
        ],
    ).withColumn(
        "valor_total_homologado",
        col("valor_total_homologado").cast("decimal(38,2)"),
    )
    contratacoes = spark.createDataFrame(
        [("c1", "2026-07-01", "6", "Pregao")],
        ["contratacao_id", "publicado_em", "modalidade_id", "modalidade"],
    )
    vinculos = spark.createDataFrame(
        [("c1", "o1")], ["contratacao_id", "orgao_id"]
    )

    gold = calcular_concentracao_fornecedores(
        itens, resultados, contratacoes, vinculos
    ).first()

    assert gold.top_3 == pytest.approx(1.0)
    assert gold.top_3 <= 1


def test_cobertura_servicos_mantem_preco_nao_publicavel(spark) -> None:
    itens = spark.createDataFrame(
        [
            ("i1", "cloud", "UNIDADE", 100.0, "nao_publicavel"),
            ("i2", "cloud", "MÊS", 200.0, "nao_publicavel"),
            ("i3", "suporte", "HORA", None, "nao_publicavel"),
        ],
        [
            "item_id",
            "categoria_servico",
            "unidade_medida",
            "valor_unitario_estimado",
            "status_preco_servico",
        ],
    )

    cobertura = calcular_cobertura_servicos(itens)
    cloud = cobertura.where("categoria_servico = 'cloud'").first()

    assert cloud.total_itens == 2
    assert cloud.unidades_distintas == 2
    assert cloud.itens_com_preco == 2
    assert cloud.status_publicacao_preco == "nao_publicavel"


def test_recorrencia_presenca_e_rede_reconciliam_relacoes(spark) -> None:
    itens = spark.createDataFrame(
        [
            ("c1", "i1", "monitor"),
            ("c2", "i2", "monitor"),
            ("c3", "i3", "servidor"),
        ],
        ["contratacao_id", "item_id", "categoria_tecnologia"],
    )
    resultados = spark.createDataFrame(
        [
            ("r1", "i1", "c1", "f1", 100.0, False),
            ("r2", "i2", "c2", "f1", 150.0, False),
            ("r3", "i3", "c3", "f2", 300.0, False),
        ],
        [
            "resultado_id",
            "item_id",
            "contratacao_id",
            "fornecedor_id",
            "valor_total_homologado",
            "cancelado",
        ],
    )
    contratacoes = spark.createDataFrame(
        [
            ("c1", "2026-05-01", "6", "Pregao"),
            ("c2", "2026-06-01", "6", "Pregao"),
            ("c3", "2026-06-02", "8", "Dispensa"),
        ],
        ["contratacao_id", "publicado_em", "modalidade_id", "modalidade"],
    )
    vinculos = spark.createDataFrame(
        [("c1", "o1", "u1"), ("c2", "o1", "u1"), ("c3", "o2", "u2")],
        ["contratacao_id", "orgao_id", "unidade_id"],
    )
    unidades = spark.createDataFrame(
        [("u1", "RJ", "Rio de Janeiro"), ("u2", "SP", "Sao Paulo")],
        ["unidade_id", "uf", "municipio"],
    )

    recorrencias = calcular_recorrencia_orgao_fornecedor(
        itens, resultados, contratacoes, vinculos
    )
    recorrencia = recorrencias.where(
        "orgao_id = 'o1' AND fornecedor_id = 'f1'"
    ).first()
    presenca = calcular_presenca_fornecedores(
        itens, resultados, contratacoes, vinculos, unidades
    ).where("fornecedor_id = 'f1'").first()
    rede = calcular_rede_orgao_fornecedor(
        itens, resultados, contratacoes, vinculos
    ).where("orgao_id = 'o1' AND fornecedor_id = 'f1'").first()

    assert recorrencia.contratacoes_distintas == 2
    assert recorrencia.periodos_distintos == 2
    assert recorrencia.dias_entre_primeira_ultima == 31
    assert recorrencias.count() == 1
    assert recorrencias.where("fornecedor_id = 'f2'").count() == 0
    assert presenca.orgaos_distintos == 1
    assert presenca.periodos_distintos == 2
    assert presenca.valor_total_homologado == pytest.approx(250.0)
    assert rede.resultados_distintos == 2
    assert rede.status_publicacao == "publicada"


def test_preco_por_categoria_e_unidade_e_medido_mas_nao_publicado(spark) -> None:
    itens = spark.createDataFrame(
        [
            ("c1", "i1", "monitor", "UN", "M"),
            ("c1", "i2", "monitor", "UN", "M"),
            ("c1", "i3", "monitor", "UN", "M"),
        ],
        [
            "contratacao_id",
            "item_id",
            "categoria_tecnologia",
            "unidade_medida",
            "material_ou_servico",
        ],
    )
    resultados = spark.createDataFrame(
        [
            ("r1", "i1", "c1", 90.0, False),
            ("r2", "i2", "c1", 100.0, False),
            ("r3", "i3", "c1", 120.0, False),
        ],
        [
            "resultado_id",
            "item_id",
            "contratacao_id",
            "valor_unitario_homologado",
            "cancelado",
        ],
    )
    contratacoes = spark.createDataFrame(
        [("c1", "2026-06-01")], ["contratacao_id", "publicado_em"]
    )

    grupo = calcular_variacao_precos(itens, resultados, contratacoes).first()

    assert grupo.observacoes == 3
    assert grupo.mediana == pytest.approx(100.0)
    assert grupo.status_publicacao == "nao_publicavel"
    assert grupo.comparabilidade_avaliada is False
    assert grupo.limitacao == "comparabilidade_desabilitada_sem_especificacao_produto"


def test_evolucao_contratual_resume_eventos_e_extensao(spark) -> None:
    contratos = spark.createDataFrame(
        [("ct1", "o1", "f1", "2026-01-01", "2026-12-31", 1000.0, 1200.0)],
        [
            "contrato_id",
            "orgao_codigo",
            "fornecedor_id",
            "vigencia_inicio",
            "vigencia_fim",
            "valor_inicial",
            "valor_global",
        ],
    )
    eventos = spark.createDataFrame(
        [
            ("e1", "ct1", "Aditivo", "2026-06-01", "2027-03-31", 200.0),
            ("e2", "ct1", "Apostilamento", "2026-07-01", None, 50.0),
        ],
        [
            "evento_contrato_id",
            "contrato_id",
            "tipo_evento",
            "data_assinatura",
            "vigencia_fim",
            "variacao_valor",
        ],
    )

    linha = calcular_evolucao_contratual(contratos, eventos).first()

    assert linha.eventos_distintos == 2
    assert linha.variacao_valor_acumulada == pytest.approx(250.0)
    assert linha.extensao_vigencia_dias == 90
    assert linha.status_publicacao == "publicada"
