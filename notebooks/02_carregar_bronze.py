# Databricks notebook source
# ruff: noqa: F821
import json
from pathlib import Path

from pyspark.sql.types import BinaryType, IntegerType, LongType, StringType, StructField, StructType

from rastro_publico.coleta.lote import preparar_lote
from rastro_publico.coleta.persistencia import append_delta_idempotente


dbutils.widgets.text("manifesto", "")
caminho_manifesto = dbutils.widgets.get("manifesto")
if not caminho_manifesto:
    raise ValueError("parametro manifesto e obrigatorio")

lote = preparar_lote(Path(caminho_manifesto))

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ops")

schema_bronze = StructType(
    [
        StructField("observacao_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("url_origem", StringType(), False),
        StructField("data_inicio_consulta", StringType(), False),
        StructField("data_fim_consulta", StringType(), False),
        StructField("modalidade", IntegerType(), False),
        StructField("pagina", IntegerType(), False),
        StructField("coletado_em_utc", StringType(), False),
        StructField("hash_payload", StringType(), False),
        StructField("payload", BinaryType(), False),
    ]
)
schema_runs = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("criado_em_utc", StringType(), False),
        StructField("status", StringType(), False),
        StructField("total_respostas", LongType(), False),
        StructField("respostas_com_payload", LongType(), False),
        StructField("total_tentativas", LongType(), False),
    ]
)
schema_requests = StructType(
    [
        StructField("request_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("modalidade", IntegerType(), False),
        StructField("pagina", IntegerType(), False),
        StructField("tentativa", IntegerType(), False),
        StructField("status_http", IntegerType(), True),
        StructField("duracao_ms", LongType(), False),
        StructField("erro", StringType(), True),
    ]
)

resultado = {
    "bronze_inseridas": append_delta_idempotente(
        spark, lote.bronze, schema_bronze, "workspace.bronze.contratacoes_raw", "observacao_id"
    ),
    "runs_inseridos": append_delta_idempotente(
        spark, lote.runs, schema_runs, "workspace.ops.ingestion_runs", "run_id"
    ),
    "requests_inseridas": append_delta_idempotente(
        spark, lote.requests, schema_requests, "workspace.ops.ingestion_requests", "request_id"
    ),
}
print(json.dumps(resultado))
