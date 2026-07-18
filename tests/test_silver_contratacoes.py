import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from rastro_publico.transformacoes.contratacoes import transformar_contratacoes


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    sessao = (
        SparkSession.builder.master("local[2]")
        .appName("silver-contratacoes-test")
        .getOrCreate()
    )
    yield sessao
    sessao.stop()


SCHEMA = StructType(
    [
        StructField(nome, StringType(), True)
        for nome in (
            "id_compra",
            "numero_controle_PNCP",
            "orgao_entidade_cnpj",
            "objeto_compra",
            "valor_total_estimado",
            "data_publicacao_pncp",
            "modalidade_id_pncp",
            "modalidade_nome",
            "data_atualizacao_pncp",
            "ind_atual",
            "contratacao_excluida",
            "_sistema_origem",
            "_source_file_id",
            "_run_id",
            "_coletado_em_utc",
        )
    ]
)


def linha(**mudancas):
    base = {
        "id_compra": "1",
        "numero_controle_PNCP": "12345678000199-1-000001/2026",
        "orgao_entidade_cnpj": "12345678000199",
        "objeto_compra": "Notebook",
        "valor_total_estimado": "100.50",
        "data_publicacao_pncp": "2026-07-15 09:00:00",
        "modalidade_id_pncp": "6",
        "modalidade_nome": "Pregão - Eletrônico",
        "data_atualizacao_pncp": "2026-07-15 10:00:00",
        "ind_atual": "True",
        "contratacao_excluida": "False",
        "_sistema_origem": "comprasgov",
        "_source_file_id": "arquivo-1",
        "_run_id": "run-1",
        "_coletado_em_utc": "2026-07-17T23:35:22+00:00",
    }
    base.update(mudancas)
    return tuple(base[campo.name] for campo in SCHEMA)


def test_seleciona_versao_mais_recente_e_tipa_campos(spark) -> None:
    bronze = spark.createDataFrame(
        [
            linha(objeto_compra="Antigo"),
            linha(
                objeto_compra="Atual",
                valor_total_estimado="200.75",
                data_atualizacao_pncp="2026-07-16 10:00:00",
                _source_file_id="arquivo-2",
            ),
        ],
        SCHEMA,
    )

    correntes, quarentena, conflitos = transformar_contratacoes(bronze)
    atual = correntes.first()

    assert quarentena.count() == 0
    assert conflitos.count() == 0
    assert atual.objeto == "Atual"
    assert str(atual.valor_total_estimado) == "200.75"
    assert atual.publicado_em.isoformat() == "2026-07-15T09:00:00"
    assert atual.modalidade_id == "6"
    assert atual.modalidade == "Pregão - Eletrônico"
    assert atual.ind_atual is True
    assert atual.contratacao_excluida is False


def test_quarentena_cnpj_ausente(spark) -> None:
    bronze = spark.createDataFrame([linha(orgao_entidade_cnpj=None)], SCHEMA)

    correntes, quarentena, conflitos = transformar_contratacoes(bronze)

    assert correntes.count() == 0
    assert conflitos.count() == 0
    assert quarentena.first().motivo_quarentena == "cnpj_orgao_ausente"


def test_hash_de_conteudo_ignora_metadados_tecnicos(spark) -> None:
    bronze = spark.createDataFrame(
        [
            linha(_source_file_id="arquivo-1", _run_id="run-1"),
            linha(_source_file_id="arquivo-2", _run_id="run-2"),
        ],
        SCHEMA,
    )

    correntes, _, conflitos = transformar_contratacoes(bronze)

    assert conflitos.count() == 0
    assert correntes.select("hash_conteudo_entidade").distinct().count() == 1


def test_empate_com_conteudo_diferente_vai_para_conflito(spark) -> None:
    bronze = spark.createDataFrame(
        [
            linha(objeto_compra="Versao A", _source_file_id="arquivo-1"),
            linha(objeto_compra="Versao B", _source_file_id="arquivo-2"),
        ],
        SCHEMA,
    )

    correntes, quarentena, conflitos = transformar_contratacoes(bronze)

    assert correntes.count() == 0
    assert quarentena.count() == 0
    assert conflitos.count() == 2
