# Databricks notebook source
# ruff: noqa: E402, F821
import hashlib
import json
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql.functions import col, countDistinct, lit, sum as spark_sum
from pyspark.sql.types import LongType, StringType, StructField, StructType


dbutils.widgets.text("arquivo", "")
dbutils.widgets.text("manifesto", "")
dbutils.widgets.text("source_root", "")
arquivo = dbutils.widgets.get("arquivo")
manifesto = dbutils.widgets.get("manifesto")
source_root = dbutils.widgets.get("source_root")
if not arquivo or not manifesto or not source_root:
    raise ValueError("arquivo, manifesto e source_root sao obrigatorios")
sys.path.insert(0, source_root)

from rastro_publico.coleta.arquivo_bronze import (
    arquivo_ja_carregado,
    preparar_csv_bronze,
    tabela_bronze,
)
from rastro_publico.coleta.persistencia import append_delta_idempotente


documento = json.loads(Path(manifesto).read_text(encoding="utf-8"))
digest = hashlib.sha256()
with Path(arquivo).open("rb") as stream:
    for bloco in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(bloco)
hash_observado = digest.hexdigest()
if hash_observado != documento["hash_arquivo"]:
    raise ValueError("hash do arquivo diverge do manifesto")
if Path(arquivo).stat().st_size != documento["tamanho_bytes"]:
    raise ValueError("tamanho do arquivo diverge do manifesto")

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ops")

source_file_id = hash_observado
metadados = {
    "source_file_id": source_file_id,
    "run_id": documento["run_id"],
    "sistema_origem": documento["sistema_origem"],
    "dataset_origem": documento["dataset_origem"],
    "coletado_em_utc": documento["coletado_em_utc"],
}
tabela_dados = tabela_bronze(documento["dataset_origem"])
entrada = preparar_csv_bronze(spark, arquivo, metadados)
linhas_entrada = entrada.count()
if arquivo_ja_carregado(spark, tabela_dados, source_file_id):
    linhas_inseridas = 0
else:
    entrada.write.format("delta").mode("append").saveAsTable(tabela_dados)
    linhas_inseridas = linhas_entrada

schema_arquivos = StructType(
    [
        StructField("source_file_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("sistema_origem", StringType(), False),
        StructField("canal_entrega", StringType(), False),
        StructField("dataset_origem", StringType(), False),
        StructField("url_origem", StringType(), False),
        StructField("caminho_arquivo", StringType(), False),
        StructField("coletado_em_utc", StringType(), False),
        StructField("data_publicacao_arquivo", StringType(), True),
        StructField("tamanho_bytes", LongType(), False),
        StructField("hash_arquivo", StringType(), False),
        StructField("total_linhas", LongType(), False),
    ]
)
schema_runs = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("criado_em_utc", StringType(), False),
        StructField("status", StringType(), False),
        StructField("total_artefatos", LongType(), False),
        StructField("total_linhas", LongType(), False),
    ]
)
schema_artefatos = StructType(
    [
        StructField("artifact_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("source_file_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("tamanho_bytes", LongType(), False),
        StructField("hash_arquivo", StringType(), False),
    ]
)
arquivo_fonte = {
    "source_file_id": source_file_id,
    "run_id": documento["run_id"],
    "sistema_origem": documento["sistema_origem"],
    "canal_entrega": documento["canal_entrega"],
    "dataset_origem": documento["dataset_origem"],
    "url_origem": documento["url_origem"],
    "caminho_arquivo": arquivo,
    "coletado_em_utc": documento["coletado_em_utc"],
    "data_publicacao_arquivo": documento["data_publicacao_arquivo"],
    "tamanho_bytes": documento["tamanho_bytes"],
    "hash_arquivo": source_file_id,
}
arquivos_inseridos = append_delta_idempotente(
    spark,
    [arquivo_fonte],
    schema_arquivos,
    "workspace.bronze.arquivos_fonte",
    "source_file_id",
)
artefatos_inseridos = append_delta_idempotente(
    spark,
    [
        {
            "artifact_id": hashlib.sha256(
                f"{documento['run_id']}|{source_file_id}".encode()
            ).hexdigest(),
            "run_id": documento["run_id"],
            "source_file_id": source_file_id,
            "status": "SUCESSO",
            "tamanho_bytes": documento["tamanho_bytes"],
            "hash_arquivo": source_file_id,
            "total_linhas": linhas_entrada,
        }
    ],
    schema_artefatos,
    "workspace.ops.ingestion_artifacts",
    "artifact_id",
)

resumo_run = (
    spark.table("workspace.ops.ingestion_artifacts")
    .where(col("run_id") == documento["run_id"])
    .agg(
        countDistinct("source_file_id").alias("total_artefatos"),
        spark_sum("total_linhas").cast("long").alias("total_linhas"),
    )
    .withColumn("run_id", lit(documento["run_id"]))
    .withColumn("criado_em_utc", lit(documento["coletado_em_utc"]))
    .withColumn("status", lit("SUCESSO"))
    .select(*[campo.name for campo in schema_runs])
)
tabela_runs = "workspace.ops.ingestion_runs_arquivo"
if spark.catalog.tableExists(tabela_runs):
    (
        DeltaTable.forName(spark, tabela_runs)
        .alias("destino")
        .merge(resumo_run.alias("origem"), "destino.run_id = origem.run_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    resumo_run.write.format("delta").saveAsTable(tabela_runs)

resultado = {
    "linhas_entrada": linhas_entrada,
    "linhas_inseridas": linhas_inseridas,
    "arquivos_inseridos": arquivos_inseridos,
    "artefatos_inseridos": artefatos_inseridos,
    "run_atualizado": 1,
}
print(json.dumps(resultado))
