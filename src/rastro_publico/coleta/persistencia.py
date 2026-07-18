import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def append_delta_idempotente(
    spark: SparkSession,
    linhas: list[dict],
    schema: StructType,
    tabela: str,
    chave: str,
) -> int:
    if not linhas:
        return 0
    if not re.fullmatch(r"[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+){1,2}", tabela):
        raise ValueError("tabela invalida")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", chave):
        raise ValueError("chave invalida")

    from delta.tables import DeltaTable

    novas = spark.createDataFrame(linhas, schema).dropDuplicates([chave])
    novas.limit(0).write.format("delta").mode("ignore").saveAsTable(tabela)
    delta = DeltaTable.forName(spark, tabela)
    (
        delta.alias("destino")
        .merge(novas.alias("origem"), f"destino.{chave} = origem.{chave}")
        .whenNotMatchedInsertAll()
        .execute()
    )
    metricas = delta.history(1).select("operationMetrics").first()[0]
    return int(metricas.get("numTargetRowsInserted", 0))
