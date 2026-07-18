# Databricks notebook source
# ruff: noqa: E402, F821
import json
import sys
from datetime import datetime, timezone

from pyspark.sql.functions import col, concat_ws, lit, sha2


dbutils.widgets.text("source_root", "")
dbutils.widgets.text("run_id", "")
source_root = dbutils.widgets.get("source_root")
run_id = dbutils.widgets.get("run_id")
if not source_root or not run_id:
    raise ValueError("source_root e run_id sao obrigatorios")
sys.path.insert(0, source_root)

from rastro_publico.operacao import avaliar_regra
from rastro_publico.transformacoes.contratos import (
    transformar_contratos,
    transformar_eventos_contrato,
    transformar_itens_contrato,
)


def materializar(dados, tabela):
    dados.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(tabela)


spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ops")

bronze_contratos = spark.table("workspace.bronze.contratos_raw")
bronze_itens = spark.table("workspace.bronze.contrato_itens_raw")
bronze_eventos = spark.table("workspace.bronze.contrato_historicos_raw")

contratos, fornecedores, contratos_quarentena, contratos_conflitos = (
    transformar_contratos(
        bronze_contratos,
        dbutils.secrets.get(scope="rastro-publico", key="fornecedor-hmac"),
    )
)
itens, itens_quarentena, itens_conflitos = transformar_itens_contrato(bronze_itens)
eventos, eventos_quarentena, eventos_conflitos = transformar_eventos_contrato(
    bronze_eventos
)

chaves_contrato = contratos.select("contrato_id")
itens_sem_contrato = itens.join(chaves_contrato, "contrato_id", "left_anti").withColumn(
    "motivo_quarentena", lit("contrato_nao_carregado")
)
eventos_sem_contrato = eventos.join(
    chaves_contrato, "contrato_id", "left_anti"
).withColumn("motivo_quarentena", lit("contrato_nao_carregado"))
itens = itens.join(chaves_contrato, "contrato_id", "inner")
eventos = eventos.join(chaves_contrato, "contrato_id", "inner")
fornecedores = fornecedores.join(
    contratos.select("fornecedor_id").distinct(), "fornecedor_id", "inner"
)

materializar(contratos, "workspace.silver.contratos")
materializar(itens, "workspace.silver.itens_contrato")
materializar(eventos, "workspace.silver.eventos_contrato")
materializar(fornecedores, "workspace.silver.fornecedores_contratos")

problemas = (
    contratos_quarentena.withColumn("entidade", lit("contrato"))
    .unionByName(
        contratos_conflitos.withColumn("entidade", lit("contrato")).withColumn(
            "motivo_quarentena", lit("empate_conteudo_divergente")
        ),
        allowMissingColumns=True,
    )
    .unionByName(
        itens_quarentena.withColumn("entidade", lit("item_contrato")),
        allowMissingColumns=True,
    )
    .unionByName(
        itens_conflitos.withColumn("entidade", lit("item_contrato")).withColumn(
            "motivo_quarentena", lit("empate_conteudo_divergente")
        ),
        allowMissingColumns=True,
    )
    .unionByName(
        eventos_quarentena.withColumn("entidade", lit("evento_contrato")),
        allowMissingColumns=True,
    )
    .unionByName(
        eventos_conflitos.withColumn("entidade", lit("evento_contrato")).withColumn(
            "motivo_quarentena", lit("empate_conteudo_divergente")
        ),
        allowMissingColumns=True,
    )
    .unionByName(
        itens_sem_contrato.withColumn("entidade", lit("item_contrato")),
        allowMissingColumns=True,
    )
    .unionByName(
        eventos_sem_contrato.withColumn("entidade", lit("evento_contrato")),
        allowMissingColumns=True,
    )
    .withColumn(
        "registro_problema_id",
        sha2(
            concat_ws(
                "|",
                "entidade",
                "source_file_id",
                "id_origem_contrato",
                "id_origem_item_contrato",
                "id_origem_evento",
                "motivo_quarentena",
                "hash_conteudo_entidade",
            ),
            256,
        ),
    )
)
materializar(problemas, "workspace.silver.contratos_problemas")

total_contratos = contratos.count()
contratos_com_itens = itens.select("contrato_id").distinct().count()
contratos_com_eventos = eventos.select("contrato_id").distinct().count()
itens_tecnologia = itens.where("categoria_tecnologia <> 'incerto'").count()
contratos_tecnologia = (
    itens.where("categoria_tecnologia <> 'incerto'")
    .select("contrato_id")
    .distinct()
    .count()
)
pf_publicos = fornecedores.where(
    (col("tipo_pessoa") == "PF") & col("identificador_publico").isNotNull()
).count()

metricas = [
    avaliar_regra(
        "contratos_duplicados",
        contratos.groupBy("contrato_id").count().where("count > 1").count(),
        total_contratos,
        0,
        "erro",
    ),
    avaliar_regra(
        "itens_contrato_duplicados",
        itens.groupBy("contrato_item_id").count().where("count > 1").count(),
        itens.count(),
        0,
        "erro",
    ),
    avaliar_regra(
        "eventos_contrato_duplicados",
        eventos.groupBy("evento_contrato_id").count().where("count > 1").count(),
        eventos.count(),
        0,
        "erro",
    ),
    avaliar_regra("cpf_publico_contrato", pf_publicos, fornecedores.count(), 0, "erro"),
    avaliar_regra(
        "itens_sem_contrato",
        itens_sem_contrato.count(),
        bronze_itens.count(),
        0,
        "alerta",
    ),
    avaliar_regra(
        "eventos_sem_contrato",
        eventos_sem_contrato.count(),
        bronze_eventos.count(),
        0,
        "alerta",
    ),
]
registrado_em = datetime.now(timezone.utc)
linhas_qualidade = [
    {
        **metrica,
        "run_id": run_id,
        "pipeline_id": "contratos",
        "recorte_id": "comprasnet_mensal",
        "registrado_em": registrado_em,
    }
    for metrica in metricas
]
qualidade = spark.createDataFrame(linhas_qualidade)
tabela_qualidade = "workspace.ops.contract_quality_results"
if spark.catalog.tableExists(tabela_qualidade):
    qualidade.createOrReplaceTempView("contract_quality_results_run")
    spark.sql(
        f"""
        MERGE INTO {tabela_qualidade} AS destino
        USING contract_quality_results_run AS origem
          ON destino.run_id = origem.run_id
         AND destino.pipeline_id = origem.pipeline_id
         AND destino.regra = origem.regra
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
else:
    qualidade.write.format("delta").saveAsTable(tabela_qualidade)

falhas = [metrica["regra"] for metrica in metricas if metrica["status"] == "FAIL"]
if falhas:
    raise RuntimeError(f"qualidade contratual bloqueante: {', '.join(falhas)}")

cobertura = spark.createDataFrame(
    [
        {
            "run_id": run_id,
            "total_contratos": total_contratos,
            "contratos_com_itens": contratos_com_itens,
            "contratos_com_eventos": contratos_com_eventos,
            "cobertura_itens": contratos_com_itens / total_contratos
            if total_contratos
            else 0.0,
            "cobertura_eventos": contratos_com_eventos / total_contratos
            if total_contratos
            else 0.0,
            "itens_sem_contrato": itens_sem_contrato.count(),
            "eventos_sem_contrato": eventos_sem_contrato.count(),
            "itens_tecnologia": itens_tecnologia,
            "contratos_tecnologia": contratos_tecnologia,
            "registrado_em": registrado_em,
        }
    ]
)
materializar(cobertura, "workspace.gold.contratos_cobertura")

print(
    json.dumps(
        {
            "bronze_contratos": bronze_contratos.count(),
            "bronze_itens": bronze_itens.count(),
            "bronze_eventos": bronze_eventos.count(),
            "silver_contratos": total_contratos,
            "silver_itens": itens.count(),
            "silver_eventos": eventos.count(),
            "fornecedores": fornecedores.count(),
            "problemas": problemas.count(),
            "contratos_com_itens": contratos_com_itens,
            "contratos_com_eventos": contratos_com_eventos,
            "contratos_tecnologia": contratos_tecnologia,
            "qualidade": metricas,
        }
    )
)
