import os
import sys

import pytest
from pyspark.sql import SparkSession

from rastro_publico.transformacoes.contratos import (
    filtrar_populacao_contratual_tecnologia,
    transformar_contratos,
    transformar_eventos_contrato,
    transformar_itens_contrato,
)


def test_populacao_contratual_restringe_contratos_eventos_e_fornecedores(spark) -> None:
    contratos = spark.createDataFrame(
        [("ct-tech", "f-tech"), ("ct-geral", "f-geral")],
        ["contrato_id", "fornecedor_id"],
    )
    itens = spark.createDataFrame(
        [("ct-tech", "monitor"), ("ct-geral", "incerto")],
        ["contrato_id", "categoria_tecnologia"],
    )
    eventos = spark.createDataFrame(
        [("e-tech", "ct-tech"), ("e-geral", "ct-geral")],
        ["evento_contrato_id", "contrato_id"],
    )
    fornecedores = spark.createDataFrame(
        [("f-tech",), ("f-geral",)],
        ["fornecedor_id"],
    )

    contratos_tech, eventos_tech, fornecedores_tech = (
        filtrar_populacao_contratual_tecnologia(
            contratos, itens, eventos, fornecedores
        )
    )

    assert [row.contrato_id for row in contratos_tech.collect()] == ["ct-tech"]
    assert [row.evento_contrato_id for row in eventos_tech.collect()] == ["e-tech"]
    assert [row.fornecedor_id for row in fornecedores_tech.collect()] == ["f-tech"]


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    sessao = SparkSession.builder.master("local[2]").appName("contratos-test").getOrCreate()
    yield sessao
    sessao.stop()


def test_contrato_tipado_e_fornecedor_pf_pseudonimizado(spark) -> None:
    bronze = spark.createDataFrame(
        [
            (
                "10",
                "00001/2026",
                "100",
                "200",
                "FISICA",
                "123.456.789-01",
                "Pessoa",
                "Processo 1",
                "Suporte de TI",
                "2026-01-01",
                "2026-01-02",
                "2026-01-01",
                "2026-12-31",
                "100.00",
                "120.00",
                "120.00",
                "Ativo",
                "2026-07-18T00:00:00+00:00",
                "arquivo-1",
            )
        ],
        [
            "id",
            "numero",
            "orgao_codigo",
            "unidade_codigo",
            "fornecedor_tipo",
            "fonecedor_cnpj_cpf_idgener",
            "fornecedor_nome",
            "processo",
            "objeto",
            "data_assinatura",
            "data_publicacao",
            "vigencia_inicio",
            "vigencia_fim",
            "valor_inicial",
            "valor_global",
            "valor_acumulado",
            "situacao",
            "_coletado_em_utc",
            "_source_file_id",
        ],
    )

    contratos, fornecedores, quarentena, conflitos = transformar_contratos(
        bronze, "segredo"
    )
    contrato = contratos.first()
    fornecedor = fornecedores.first()

    assert contrato.contrato_id
    assert contrato.valor_global == 120
    assert fornecedor.tipo_pessoa == "PF"
    assert fornecedor.identificador_publico is None
    assert len(fornecedor.fornecedor_id) == 64
    assert quarentena.count() == 0
    assert conflitos.count() == 0


def test_contrato_quarentena_data_e_valor_invalidos(spark) -> None:
    bronze = spark.createDataFrame(
        [
            (
                "10",
                "1",
                "100",
                "200",
                "JURIDICA",
                "12345678000199",
                "Fornecedor",
                "2026-12-31",
                "2026-01-01",
                "-1",
                "2026-07-18",
                "a1",
            )
        ],
        [
            "id",
            "numero",
            "orgao_codigo",
            "unidade_codigo",
            "fornecedor_tipo",
            "fonecedor_cnpj_cpf_idgener",
            "fornecedor_nome",
            "vigencia_inicio",
            "vigencia_fim",
            "valor_global",
            "_coletado_em_utc",
            "_source_file_id",
        ],
    )

    _, _, quarentena, _ = transformar_contratos(bronze, "segredo")

    assert quarentena.first().motivo_quarentena == "valor_global_negativo"


def test_contrato_prioriza_data_do_snapshot_oficial(spark) -> None:
    bronze = spark.createDataFrame(
        [
            (
                "10",
                "JURIDICA",
                "12345678000199",
                "2025-08-06T12:30:54+00:00",
                "2026-07-18T05:31:35+00:00",
                "a1",
            )
        ],
        [
            "id",
            "fornecedor_tipo",
            "fonecedor_cnpj_cpf_idgener",
            "_data_publicacao_arquivo",
            "_coletado_em_utc",
            "_source_file_id",
        ],
    )

    contrato = transformar_contratos(bronze, "segredo")[0].first()

    assert str(contrato.atualizado_em).startswith("2025-08-06")


def test_item_contratual_tipado_e_classificado(spark) -> None:
    bronze = spark.createDataFrame(
        [
            (
                "20",
                "10",
                "Serviço",
                "Software como Serviço - SaaS",
                "2",
                "50",
                "100",
                "1",
                "{'date': '2026-01-01 00:00:00.000000', 'timezone_type': 3}",
                "2026-07-18",
                "a1",
            )
        ],
        [
            "id",
            "contrato_id",
            "tipo_id",
            "descricao_complementar",
            "quantidade",
            "valorunitario",
            "valortotal",
            "numero_item_compra",
            "data_inicio_item",
            "_coletado_em_utc",
            "_source_file_id",
        ],
    )

    itens, quarentena, conflitos = transformar_itens_contrato(bronze)
    item = itens.first()

    assert item.contrato_id
    assert item.categoria_tecnologia == "servico_cloud"
    assert str(item.data_inicio_item) == "2026-01-01 00:00:00"
    assert quarentena.count() == 0
    assert conflitos.count() == 0


def test_evento_preserva_versao_mais_recente_e_variacao(spark) -> None:
    colunas = [
        "id",
        "contrato_id",
        "tipo",
        "qualificacao_termo",
        "observacao",
        "vigencia_inicio",
        "vigencia_fim",
        "valor_global",
        "novo_valor_global",
        "situacao_contrato",
        "situacao_termo",
        "criado_em",
        "alterado_em",
        "_coletado_em_utc",
        "_source_file_id",
    ]
    bronze = spark.createDataFrame(
        [
            (
                "30",
                "10",
                "Termo Aditivo",
                "VIGÊNCIA",
                "antigo",
                "2026-01-01",
                "2026-12-31",
                "100",
                "110",
                "Ativo",
                "Ativo",
                "2026-01-01",
                "2026-02-01",
                "2026-07-18",
                "a1",
            ),
            (
                "30",
                "10",
                "Termo Aditivo",
                "VIGÊNCIA",
                "corrigido",
                "2026-01-01",
                "2027-12-31",
                "100",
                "120",
                "Ativo",
                "Ativo",
                "2026-01-01",
                "2026-03-01",
                "2026-07-18",
                "a2",
            ),
        ],
        colunas,
    )

    eventos, quarentena, conflitos = transformar_eventos_contrato(bronze)
    evento = eventos.first()

    assert eventos.count() == 1
    assert evento.observacao == "corrigido"
    assert evento.variacao_valor == 20
    assert quarentena.count() == 0
    assert conflitos.count() == 0
