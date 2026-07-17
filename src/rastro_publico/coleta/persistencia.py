from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def filtrar_novas_linhas(entrada: DataFrame, existente: DataFrame, chave: str) -> DataFrame:
    return entrada.join(existente.select(chave).dropDuplicates(), chave, "left_anti")


def append_delta_idempotente(
    spark: SparkSession,
    linhas: list[dict],
    schema: StructType,
    tabela: str,
    chave: str,
) -> int:
    if not linhas:
        return 0
    novas = spark.createDataFrame(linhas, schema)
    if spark.catalog.tableExists(tabela):
        novas = filtrar_novas_linhas(novas, spark.table(tabela), chave)
    quantidade = novas.count()
    if quantidade:
        novas.write.format("delta").mode("append").saveAsTable(tabela)
    return quantidade
