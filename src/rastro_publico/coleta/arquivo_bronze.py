from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    lit,
    max as spark_max,
    sum as spark_sum,
)
from pyspark.sql.types import LongType, StringType, StructField, StructType

TABELAS_BRONZE = {
    "VW_FT_PNCP_COMPRA": "workspace.bronze.contratacoes_raw",
    "VW_FT_PNCP_COMPRA_ITEM": "workspace.bronze.itens_raw",
    "VW_DM_PNCP_ITEM_RESULTADO": "workspace.bronze.resultados_raw",
    "CONTRATOS_CONTRATOS": "workspace.bronze.contratos_raw",
    "CONTRATOS_ITENS": "workspace.bronze.contrato_itens_raw",
    "CONTRATOS_HISTORICOS": "workspace.bronze.contrato_historicos_raw",
}

SCHEMA_INGESTION_ARTIFACTS = StructType(
    [
        StructField("artifact_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("source_file_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("tamanho_bytes", LongType(), False),
        StructField("hash_arquivo", StringType(), False),
        StructField("total_linhas", LongType(), False),
    ]
)


def tabela_bronze(dataset_origem: str) -> str:
    try:
        return TABELAS_BRONZE[dataset_origem]
    except KeyError as erro:
        raise ValueError(f"dataset nao suportado: {dataset_origem}") from erro


def preparar_csv_bronze(
    spark: SparkSession, caminho: str, metadados: dict[str, str]
) -> DataFrame:
    dataframe = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(caminho)
    )
    for nome, valor in metadados.items():
        dataframe = dataframe.withColumn(f"_{nome}", lit(valor))
    return dataframe


def arquivo_ja_carregado(spark: SparkSession, tabela: str, source_file_id: str) -> bool:
    if not spark.catalog.tableExists(tabela):
        return False
    return bool(
        spark.table(tabela).where(col("_source_file_id") == source_file_id).limit(1).count()
    )


def resumir_run_artefatos(
    artefatos: DataFrame, run_id: str, criado_em_utc: str
) -> DataFrame:
    return (
        artefatos.where(col("run_id") == run_id)
        .dropDuplicates(["artifact_id"])
        .agg(
            countDistinct("source_file_id").alias("total_artefatos"),
            spark_sum("total_linhas").cast("long").alias("total_linhas"),
        )
        .withColumn("run_id", lit(run_id))
        .withColumn("criado_em_utc", lit(criado_em_utc))
        .withColumn("status", lit("SUCESSO"))
        .select(
            "run_id",
            "criado_em_utc",
            "status",
            "total_artefatos",
            "total_linhas",
        )
    )


def garantir_coluna_total_linhas(spark: SparkSession, tabela: str) -> bool:
    if not spark.catalog.tableExists(tabela) or "total_linhas" in spark.table(
        tabela
    ).columns:
        return False
    spark.sql(f"ALTER TABLE {tabela} ADD COLUMNS (total_linhas BIGINT)")
    return True


def migrar_total_linhas_ingestao(spark: SparkSession) -> None:
    tabela_arquivos = "workspace.bronze.arquivos_fonte"
    tabela_artefatos = "workspace.ops.ingestion_artifacts"
    garantir_coluna_total_linhas(spark, tabela_arquivos)
    garantir_coluna_total_linhas(spark, tabela_artefatos)
    if not spark.catalog.tableExists(tabela_arquivos):
        return

    contagens = []
    for tabela in set(TABELAS_BRONZE.values()):
        if spark.catalog.tableExists(tabela):
            contagens.append(
                spark.table(tabela)
                .groupBy("_source_file_id")
                .count()
                .select(
                    col("_source_file_id").alias("source_file_id"),
                    col("count").cast("long").alias("total_linhas"),
                )
            )
    if not contagens:
        return

    totais = contagens[0]
    for parte in contagens[1:]:
        totais = totais.unionByName(parte)
    totais = totais.groupBy("source_file_id").agg(
        spark_max("total_linhas").alias("total_linhas")
    )

    from delta.tables import DeltaTable

    (
        DeltaTable.forName(spark, tabela_arquivos)
        .alias("destino")
        .merge(totais.alias("origem"), "destino.source_file_id = origem.source_file_id")
        .whenMatchedUpdate(set={"total_linhas": "origem.total_linhas"})
        .execute()
    )
    if spark.catalog.tableExists(tabela_artefatos):
        totais_artefatos = spark.table(tabela_arquivos).select(
            "source_file_id", "total_linhas"
        ).where(col("total_linhas").isNotNull()).dropDuplicates(["source_file_id"])
        (
            DeltaTable.forName(spark, tabela_artefatos)
            .alias("destino")
            .merge(
                totais_artefatos.alias("origem"),
                "destino.source_file_id = origem.source_file_id",
            )
            .whenMatchedUpdate(set={"total_linhas": "origem.total_linhas"})
            .execute()
        )
