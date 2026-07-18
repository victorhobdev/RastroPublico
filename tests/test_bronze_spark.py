import pytest

from rastro_publico.coleta.arquivo_bronze import (
    SCHEMA_INGESTION_ARTIFACTS,
    arquivo_ja_carregado,
    garantir_coluna_total_linhas,
    preparar_csv_bronze,
    resumir_run_artefatos,
    tabela_bronze,
)


def test_prepara_csv_bronze_com_proveniencia(spark, tmp_path) -> None:
    caminho = tmp_path / "compras.csv"
    caminho.write_text(
        'id_compra,objeto\n1,"Notebook ""Pro""\ncom suporte"\n2,Monitor\n',
        encoding="utf-8",
    )

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
    assert resultado.where("id_compra = '1'").first().objeto == 'Notebook "Pro"\ncom suporte'


def test_detecta_arquivo_ja_carregado(spark, monkeypatch) -> None:
    existente = spark.createDataFrame([("hash-1",)], ["_source_file_id"])
    monkeypatch.setattr(spark.catalog, "tableExists", lambda _tabela: True)
    monkeypatch.setattr(spark, "table", lambda _tabela: existente)

    assert arquivo_ja_carregado(spark, "bronze.contratacoes_raw", "hash-1")
    assert not arquivo_ja_carregado(spark, "bronze.contratacoes_raw", "hash-2")


def test_mapeia_somente_datasets_bronze_aprovados() -> None:
    assert tabela_bronze("VW_FT_PNCP_COMPRA_ITEM") == "workspace.bronze.itens_raw"
    assert tabela_bronze("VW_DM_PNCP_ITEM_RESULTADO") == "workspace.bronze.resultados_raw"
    assert tabela_bronze("CONTRATOS_CONTRATOS") == "workspace.bronze.contratos_raw"
    assert tabela_bronze("CONTRATOS_ITENS") == "workspace.bronze.contrato_itens_raw"
    assert (
        tabela_bronze("CONTRATOS_HISTORICOS")
        == "workspace.bronze.contrato_historicos_raw"
    )

    with pytest.raises(ValueError, match="dataset nao suportado"):
        tabela_bronze("tabela_injetada")


def test_schema_e_resumo_preservam_contagem_de_cada_artefato(spark) -> None:
    assert "total_linhas" in SCHEMA_INGESTION_ARTIFACTS.fieldNames()
    artefatos = spark.createDataFrame(
        [
            ("a1", "run-1", "f1", "SUCESSO", 10, "f1", 2),
            ("a2", "run-1", "f2", "SUCESSO", 20, "f2", 3),
            ("a3", "run-2", "f3", "SUCESSO", 30, "f3", 7),
        ],
        SCHEMA_INGESTION_ARTIFACTS,
    )

    resumo = resumir_run_artefatos(artefatos, "run-1", "2026-07-18").first()

    assert resumo.total_artefatos == 2
    assert resumo.total_linhas == 5


def test_resumo_ignora_reexecucao_do_mesmo_artefato(spark) -> None:
    artefatos = spark.createDataFrame(
        [
            ("a1", "run-1", "f1", "SUCESSO", 10, "f1", 2),
            ("a1", "run-1", "f1", "SUCESSO", 10, "f1", 2),
        ],
        SCHEMA_INGESTION_ARTIFACTS,
    )

    resumo = resumir_run_artefatos(artefatos, "run-1", "2026-07-18").first()

    assert resumo.total_artefatos == 1
    assert resumo.total_linhas == 2


def test_migracao_de_schema_e_idempotente(monkeypatch) -> None:
    class Tabela:
        columns = ["artifact_id"]

    class Catalogo:
        @staticmethod
        def tableExists(_tabela):
            return True

    class Spark:
        catalog = Catalogo()

        @staticmethod
        def table(_tabela):
            return Tabela()

        @staticmethod
        def sql(comando):
            comandos.append(comando)
            Tabela.columns.append("total_linhas")

    comandos = []
    monkeypatch.setattr(Tabela, "columns", ["artifact_id"])

    assert garantir_coluna_total_linhas(Spark(), "workspace.ops.ingestion_artifacts")
    assert not garantir_coluna_total_linhas(
        Spark(), "workspace.ops.ingestion_artifacts"
    )
    assert comandos == [
        "ALTER TABLE workspace.ops.ingestion_artifacts "
        "ADD COLUMNS (total_linhas BIGINT)"
    ]
