import os
import sys

import pytest
from pyspark.sql import SparkSession

from rastro_publico.transformacoes.contexto import (
    enriquecer_fornecedores_contexto,
    resumir_qsa,
    transformar_empresas_cnpj,
    transformar_sancoes,
)


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    sessao = (
        SparkSession.builder.master("local[2]").appName("contexto-test").getOrCreate()
    )
    yield sessao
    sessao.stop()


def test_contexto_cnpj_qsa_e_sancoes_preserva_semantica(spark) -> None:
    empresas_raw = spark.createDataFrame(
        [("12345678", "Fornecedor", "2062", "49", "1000,50", "03", "")],
        [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo_responsavel",
        ],
    )
    qsa_raw = spark.createDataFrame(
        [
            ("12345678", "2", "49", "20200101", "", "05", "4"),
            ("12345678", "1", "22", "20210101", "", "", ""),
        ],
        [
            "cnpj_basico",
            "tipo_socio",
            "qualificacao_socio",
            "data_entrada_sociedade",
            "pais",
            "qualificacao_representante",
            "faixa_etaria",
        ],
    )
    sancoes_raw = spark.createDataFrame(
        [
            (
                "CEIS",
                "99",
                "12345678000199",
                "Suspensao",
                "",
                "01/01/2026",
                "31/12/2026",
                "02/01/2026",
                "Nacional",
                "Orgao",
                "RJ",
                "FEDERAL",
                "03/01/2026",
            )
        ],
        [
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
        ],
    )
    fornecedores = spark.createDataFrame(
        [("f1", "12345678000199", "Fornecedor")],
        ["fornecedor_id", "identificador_publico", "nome_fornecedor"],
    )

    empresas = transformar_empresas_cnpj(empresas_raw)
    qsa = resumir_qsa(qsa_raw)
    sancoes = transformar_sancoes(sancoes_raw)
    contexto = enriquecer_fornecedores_contexto(fornecedores, empresas, qsa, sancoes)
    linha = contexto.first()

    assert empresas.first().capital_social == pytest.approx(1000.5)
    assert qsa.first().registros_socios == 2
    assert qsa.first().socios_pessoa_juridica == 1
    assert sancoes.first().sancoes_distintas == 1
    assert linha.sancoes_ceis == 1
    assert linha.sancoes_cnep == 0
    assert linha.status_publicacao == "publicada"
    assert linha.interpretacao == "contexto_cadastral_nao_indica_irregularidade"
