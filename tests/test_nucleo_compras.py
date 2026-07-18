import os
import sys

import pytest
from pyspark.sql import SparkSession

from rastro_publico.transformacoes.nucleo import (
    classificar_equipamentos,
    pseudonimizar_identificador,
    transformar_dimensoes,
    transformar_itens,
    transformar_resultados,
    transformar_vinculos_contratacao,
)


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    sessao = (
        SparkSession.builder.master("local[2]")
        .appName("nucleo-compras-test")
        .getOrCreate()
    )
    yield sessao
    sessao.stop()


def test_item_tem_grao_logico_e_quarentena_quantidade(spark) -> None:
    bronze = spark.createDataFrame(
        [
            (
                "surrogate-1",
                "item-1",
                "controle-1",
                "1",
                "Notebook",
                "",
                "M",
                "1",
                "UN",
                "100",
                "100",
                "2026-07-15 10:00:00",
                "arq-1",
            ),
            (
                "surrogate-2",
                "item-1",
                "controle-1",
                "1",
                "Notebook",
                "",
                "M",
                "1",
                "UN",
                "100",
                "100",
                "2026-07-15 10:00:00",
                "arq-1",
            ),
            (
                "surrogate-3",
                "item-2",
                "controle-1",
                "2",
                "Monitor",
                "",
                "M",
                "0",
                "UN",
                "200",
                "0",
                "2026-07-15 10:00:00",
                "arq-1",
            ),
        ],
        [
            "srk_pncp_item_compra",
            "id_compra_item",
            "numero_controle_PNCP_compra",
            "numero_item_pncp",
            "descricao_resumida",
            "descricao_detalhada",
            "material_ou_servico",
            "quantidade",
            "unidade_medida",
            "valor_unitario_estimado",
            "valor_total",
            "data_atualizacao_pncp",
            "_source_file_id",
        ],
    )

    itens, quarentena, conflitos = transformar_itens(bronze)

    assert itens.count() == 1
    assert itens.first().id_origem_item == "item-1"
    assert quarentena.first().motivo_quarentena == "quantidade_nao_positiva"
    assert conflitos.count() == 0


def test_item_mais_novo_substitui_versao_antiga_e_preserva_situacao(spark) -> None:
    colunas = [
        "id_compra_item",
        "numero_controle_PNCP_compra",
        "numero_item_pncp",
        "descricao_resumida",
        "descricao_detalhada",
        "material_ou_servico",
        "quantidade",
        "unidade_medida",
        "valor_unitario_estimado",
        "valor_total",
        "situacao_compra_item_nome",
        "data_atualizacao_pncp",
        "_source_file_id",
    ]
    bronze = spark.createDataFrame(
        [
            (
                "i1",
                "controle-1",
                "1",
                "Notebook",
                "",
                "M",
                "1",
                "UN",
                "100",
                "100",
                "Ativo",
                "2026-07-14",
                "a1",
            ),
            (
                "i1",
                "controle-1",
                "1",
                "Notebook",
                "",
                "M",
                "2",
                "UN",
                "100",
                "200",
                "Cancelado",
                "2026-07-15",
                "a2",
            ),
        ],
        colunas,
    )

    item = transformar_itens(bronze)[0].first()

    assert item.quantidade == 2
    assert item.situacao_item == "Cancelado"
    assert item.cancelado is True


def test_resultado_remove_cpf_e_mantem_vinculo(spark) -> None:
    bronze = spark.createDataFrame(
        [
            (
                "r1",
                "item-1",
                "controle-1",
                "1",
                "1",
                "PF",
                "BRA",
                "12345678901",
                "Pessoa",
                "1",
                "100",
                "100",
                "2026-07-15 10:00:00",
                "arq-1",
            )
        ],
        [
            "srk_item_resultado",
            "id_compra_item",
            "numero_controle_PNCP_compra",
            "numero_item_pncp",
            "sequencial_resultado",
            "tipo_pessoa",
            "codigo_pais",
            "ni_fornecedor",
            "nome_razao_social_fornecedor",
            "quantidade_homologada",
            "valor_unitario_homologado",
            "valor_total_homologado",
            "data_atualizacao_pncp",
            "_source_file_id",
        ],
    )

    resultados, fornecedores, quarentena, conflitos = transformar_resultados(
        bronze, "segredo-teste"
    )

    assert resultados.count() == 1
    assert "ni_fornecedor" not in resultados.columns
    assert fornecedores.first().identificador_publico is None
    assert fornecedores.first().fornecedor_id == pseudonimizar_identificador(
        "segredo-teste", "PF|BRA|12345678901"
    )
    assert quarentena.count() == 0
    assert conflitos.count() == 0


def test_resultado_preserva_cancelamento_sem_expor_identificador(spark) -> None:
    bronze = spark.createDataFrame(
        [
            (
                "r1",
                "item-1",
                "controle-1",
                "1",
                "1",
                "PJ",
                "BRA",
                "12345678000199",
                "Fornecedor",
                "1",
                "100",
                "100",
                "2026-07-15",
                "2026-07-16",
                "Correção do resultado",
                "Cancelado",
                "2",
                "a1",
            )
        ],
        [
            "srk_item_resultado",
            "id_compra_item",
            "numero_controle_PNCP_compra",
            "numero_item_pncp",
            "sequencial_resultado",
            "tipo_pessoa",
            "codigo_pais",
            "ni_fornecedor",
            "nome_razao_social_fornecedor",
            "quantidade_homologada",
            "valor_unitario_homologado",
            "valor_total_homologado",
            "data_atualizacao_pncp",
            "data_cancelamento_pncp",
            "motivo_cancelamento",
            "situacao_compra_item_resultado_nome",
            "situacao_compra_item_resultado_id",
            "_source_file_id",
        ],
    )

    resultado = transformar_resultados(bronze, "segredo")[0].first()

    assert resultado.cancelado is True
    assert resultado.motivo_cancelamento == "Correção do resultado"
    assert "identificador_normalizado" not in resultado.asDict()


def test_dimensoes_selecionam_versao_mais_recente(spark) -> None:
    bronze = spark.createDataFrame(
        [
            (
                "123",
                "Órgão antigo",
                "F",
                "E",
                "10",
                "Unidade antiga",
                "RJ",
                "Rio",
                "3304557",
                "2026-07-14",
                "a1",
            ),
            (
                "123",
                "Órgão atual",
                "F",
                "E",
                "10",
                "Unidade atual",
                "RJ",
                "Rio",
                "3304557",
                "2026-07-15",
                "a2",
            ),
        ],
        [
            "orgao_entidade_cnpj",
            "orgao_entidade_razao_social",
            "orgao_entidade_esfera_id",
            "orgao_entidade_poder_id",
            "unidade_orgao_codigo_unidade",
            "unidade_orgao_nome_unidade",
            "unidade_orgao_uf_sigla",
            "unidade_orgao_municipio_nome",
            "unidade_orgao_codigo_ibge",
            "data_atualizacao_pncp",
            "_source_file_id",
        ],
    )

    orgaos, unidades = transformar_dimensoes(bronze)

    assert orgaos.count() == 1
    assert orgaos.first().nome_orgao == "Órgão atual"
    assert unidades.count() == 1
    assert unidades.first().nome_unidade == "Unidade atual"


def test_vinculo_preserva_contratacao_orgao_e_unidade(spark) -> None:
    bronze = spark.createDataFrame(
        [("controle-1", "123", "10", "2026-07-15", "a1")],
        [
            "numero_controle_PNCP",
            "orgao_entidade_cnpj",
            "unidade_orgao_codigo_unidade",
            "data_atualizacao_pncp",
            "_source_file_id",
        ],
    )

    vinculo = transformar_vinculos_contratacao(bronze).first()

    assert vinculo.contratacao_id
    assert vinculo.orgao_id
    assert vinculo.unidade_id


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
    itens = spark.createDataFrame(
        [("item-1", descricao, "M")],
        ["item_id", "descricao", "material_ou_servico"],
    )

    classificado = classificar_equipamentos(itens).first()

    assert classificado.categoria_tecnologia == categoria
    assert classificado.versao_regra == "equipamentos_v2"


@pytest.mark.parametrize(
    "descricao",
    [
        "Cessão de direitos sobre programas de computador",
        "Cartucho tinta impressora HP",
        "Administração de estágio universitário monitor",
        "Transporte para servidor público",
        "Microfone direcional tipo switch",
    ],
)
def test_classificacao_rejeita_contextos_falso_positivos(spark, descricao) -> None:
    itens = spark.createDataFrame(
        [("item-1", descricao, "M")],
        ["item_id", "descricao", "material_ou_servico"],
    )

    assert classificar_equipamentos(itens).first().categoria_tecnologia == "incerto"


def test_classificacao_rejeita_servico_mesmo_com_nome_de_equipamento(spark) -> None:
    itens = spark.createDataFrame(
        [("item-1", "Servidor rack", "S")],
        ["item_id", "descricao", "material_ou_servico"],
    )

    assert classificar_equipamentos(itens).first().categoria_tecnologia == "incerto"
