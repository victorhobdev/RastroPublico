import os
import sys

import pytest
from pyspark.sql import SparkSession

from rastro_publico.transformacoes.nucleo import (
    classificar_equipamentos,
    pseudonimizar_identificador,
    transformar_itens,
    transformar_resultados,
)


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    sessao = SparkSession.builder.master("local[2]").appName("nucleo-compras-test").getOrCreate()
    yield sessao
    sessao.stop()


def test_item_tem_grao_logico_e_quarentena_quantidade(spark) -> None:
    bronze = spark.createDataFrame(
        [
            ("surrogate-1", "item-1", "controle-1", "1", "Notebook", "", "M", "1", "UN", "100", "100", "2026-07-15 10:00:00", "arq-1"),
            ("surrogate-2", "item-1", "controle-1", "1", "Notebook", "", "M", "1", "UN", "100", "100", "2026-07-15 10:00:00", "arq-1"),
            ("surrogate-3", "item-2", "controle-1", "2", "Monitor", "", "M", "0", "UN", "200", "0", "2026-07-15 10:00:00", "arq-1"),
        ],
        ["srk_pncp_item_compra", "id_compra_item", "numero_controle_PNCP_compra", "numero_item_pncp", "descricao_resumida", "descricao_detalhada", "material_ou_servico", "quantidade", "unidade_medida", "valor_unitario_estimado", "valor_total", "data_atualizacao_pncp", "_source_file_id"],
    )

    itens, quarentena, conflitos = transformar_itens(bronze)

    assert itens.count() == 1
    assert itens.first().id_origem_item == "item-1"
    assert quarentena.first().motivo_quarentena == "quantidade_nao_positiva"
    assert conflitos.count() == 0


def test_resultado_remove_cpf_e_mantem_vinculo(spark) -> None:
    bronze = spark.createDataFrame(
        [("r1", "item-1", "controle-1", "1", "1", "PF", "BRA", "12345678901", "Pessoa", "1", "100", "100", "2026-07-15 10:00:00", "arq-1")],
        ["srk_item_resultado", "id_compra_item", "numero_controle_PNCP_compra", "numero_item_pncp", "sequencial_resultado", "tipo_pessoa", "codigo_pais", "ni_fornecedor", "nome_razao_social_fornecedor", "quantidade_homologada", "valor_unitario_homologado", "valor_total_homologado", "data_atualizacao_pncp", "_source_file_id"],
    )

    resultados, fornecedores, quarentena, conflitos = transformar_resultados(bronze, "segredo-teste")

    assert resultados.count() == 1
    assert "ni_fornecedor" not in resultados.columns
    assert fornecedores.first().identificador_publico is None
    assert fornecedores.first().fornecedor_id == pseudonimizar_identificador("segredo-teste", "PF|BRA|12345678901")
    assert quarentena.count() == 0
    assert conflitos.count() == 0


@pytest.mark.parametrize(
    ("descricao", "categoria"),
    [
        ("Notebook corporativo", "computador_notebook"),
        ("Monitor LED 24", "monitor"),
        ("Impressora multifuncional", "impressora_scanner"),
        ("Servidor rack", "servidor"),
        ("Switch gerenciável 48 portas", "equipamento_rede"),
        ("Cabo USB", "incerto"),
    ],
)
def test_classificacao_inicial_equipamentos(spark, descricao, categoria) -> None:
    itens = spark.createDataFrame([("item-1", descricao)], ["item_id", "descricao"])

    classificado = classificar_equipamentos(itens).first()

    assert classificado.categoria_tecnologia == categoria
    assert classificado.versao_regra == "equipamentos_v1"
