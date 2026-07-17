from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit


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
