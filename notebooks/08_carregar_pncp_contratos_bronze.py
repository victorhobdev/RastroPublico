# Databricks notebook source
# ruff: noqa: E402, F821
import json
import sys
from pathlib import Path

from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


dbutils.widgets.text("manifesto_vinculos", "")
dbutils.widgets.text("manifesto_capacidades", "")
dbutils.widgets.text("source_root", "")
manifestos_widgets = [
    dbutils.widgets.get("manifesto_vinculos"),
    dbutils.widgets.get("manifesto_capacidades"),
]
source_root = dbutils.widgets.get("source_root")
if not source_root or any(not caminho for caminho in manifestos_widgets):
    raise ValueError("manifestos e source_root sao obrigatorios")
manifestos = [Path(caminho) for caminho in manifestos_widgets]
sys.path.insert(0, source_root)

from rastro_publico.coleta.persistencia import append_delta_idempotente
from rastro_publico.coleta.vinculos_pncp import preparar_manifestos_bronze


bronze, operacao = preparar_manifestos_bronze(manifestos)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ops")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

schema_bronze = StructType(
    [
        StructField("observacao_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("capacidade", StringType(), False),
        StructField("numero_controle_pncp", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("url_origem", StringType(), False),
        StructField("pagina", IntegerType(), False),
        StructField("coletado_em_utc", StringType(), False),
        StructField("hash_payload", StringType(), False),
        StructField("payload", BinaryType(), False),
    ]
)
schema_operacao = StructType(
    [
        StructField("request_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("capacidade", StringType(), False),
        StructField("numero_controle_pncp", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("url_origem", StringType(), False),
        StructField("pagina", IntegerType(), False),
        StructField("status_http", IntegerType(), False),
        StructField("tentativas", IntegerType(), False),
        StructField("coletado_em_utc", StringType(), False),
    ]
)

resultado = {
    "bronze_inseridas": append_delta_idempotente(
        spark,
        bronze,
        schema_bronze,
        "workspace.bronze.contratos_pncp_raw",
        "observacao_id",
    ),
    "requests_inseridas": append_delta_idempotente(
        spark,
        operacao,
        schema_operacao,
        "workspace.ops.contract_source_requests",
        "request_id",
    ),
    "payloads_entrada": len(bronze),
    "requests_entrada": len(operacao),
}

vinculos = json.loads(manifestos[0].read_text(encoding="utf-8"))
capacidades = json.loads(manifestos[1].read_text(encoding="utf-8"))
cobertura = spark.createDataFrame(
    [
        {
            "run_id": vinculos["run_id"],
            "contratacoes_avaliadas": vinculos["contratacoes_avaliadas"],
            "contratacoes_com_vinculo": vinculos["contratacoes_com_vinculo"],
            "contratos_encontrados": vinculos["contratos_encontrados"],
            "respostas_404": vinculos["respostas_404"],
            "respostas_422": vinculos["respostas_422"],
            "taxa_vinculo": vinculos["taxa_vinculo"],
            "detalhes_disponiveis": capacidades["detalhes_disponiveis"],
            "contratos_com_historico": capacidades["contratos_com_historico"],
            "eventos_historicos": capacidades["eventos_historicos"],
            "contratos_com_termos": capacidades["contratos_com_termos"],
            "termos_encontrados": capacidades["termos_encontrados"],
            "cenario_contratual": "C3",
            "estado_capacidade": "publicada",
            "limitacao": "vinculo_contratacao_contrato_parcial; cobertura sempre obrigatoria",
            "registrado_em_utc": capacidades["criado_em_utc"],
        }
    ]
)
cobertura.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("workspace.gold.vinculo_contrato_cobertura")

dbutils.notebook.exit(json.dumps(resultado))
