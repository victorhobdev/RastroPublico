# Databricks notebook source
# ruff: noqa: E402, F821
import json
import sys

from pyspark.sql.functions import col, concat_ws, lit, sha2


dbutils.widgets.text("source_root", "")
source_root = dbutils.widgets.get("source_root")
if not source_root:
    raise ValueError("source_root e obrigatorio")
sys.path.insert(0, source_root)

from rastro_publico.transformacoes.contratacoes import transformar_contratacoes


spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
bronze = spark.table("workspace.bronze.contratacoes_raw")
correntes, quarentena, conflitos_bronze = transformar_contratacoes(bronze)
tabela = "workspace.silver.contratacoes"

conflitos_bronze = conflitos_bronze.withColumn("motivo", lit("empate_conteudo_divergente"))
if spark.catalog.tableExists(tabela):
    destino = spark.table(tabela)
    conflitos_destino = (
        correntes.alias("s")
        .join(
            destino.alias("t"),
            (col("s.contratacao_id") == col("t.contratacao_id"))
            & (col("s.atualizado_em") == col("t.atualizado_em"))
            & (col("s.hash_conteudo_entidade") != col("t.hash_conteudo_entidade")),
            "inner",
        )
        .select("s.*")
        .withColumn("motivo", lit("empate_com_silver_divergente"))
    )
    conflitos = conflitos_bronze.unionByName(conflitos_destino)
    elegiveis = correntes.join(
        conflitos_destino.select("contratacao_id").distinct(), "contratacao_id", "left_anti"
    )
    elegiveis.createOrReplaceTempView("source_contratacoes")
    spark.sql(
        f"""
        MERGE INTO {tabela} AS t
        USING source_contratacoes AS s
        ON t.contratacao_id = s.contratacao_id
        WHEN MATCHED AND s.atualizado_em > t.atualizado_em THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
else:
    conflitos = conflitos_bronze
    correntes.write.format("delta").mode("overwrite").saveAsTable(tabela)

problemas = quarentena.withColumn("motivo", col("motivo_quarentena")).drop("motivo_quarentena")
problemas = problemas.unionByName(conflitos, allowMissingColumns=True).withColumn(
    "registro_problema_id",
    sha2(concat_ws("|", "source_file_id", "id_origem", "motivo", "hash_conteudo_entidade"), 256),
)
tabela_problemas = "workspace.silver.contratacoes_problemas"
if spark.catalog.tableExists(tabela_problemas):
    novos = problemas.join(
        spark.table(tabela_problemas).select("registro_problema_id"),
        "registro_problema_id",
        "left_anti",
    )
    novos.write.format("delta").mode("append").saveAsTable(tabela_problemas)
else:
    problemas.write.format("delta").mode("overwrite").saveAsTable(tabela_problemas)

resultado = {
    "bronze": bronze.count(),
    "silver": spark.table(tabela).count(),
    "quarentena": quarentena.count(),
    "conflitos": conflitos.count(),
    "problemas": spark.table(tabela_problemas).count(),
}
print(json.dumps(resultado))
