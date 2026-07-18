# Databricks notebook source
# ruff: noqa: E402, F821
import json
import sys

from pyspark.sql.functions import concat_ws, lit, sha2


dbutils.widgets.text("source_root", "")
source_root = dbutils.widgets.get("source_root")
if not source_root:
    raise ValueError("source_root e obrigatorio")
sys.path.insert(0, source_root)

from rastro_publico.transformacoes.nucleo import (
    classificar_equipamentos,
    transformar_dimensoes,
    transformar_itens,
    transformar_resultados,
    transformar_vinculos_contratacao,
)


def materializar_snapshot(dados, tabela):
    dados.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(tabela)


spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
bronze_itens = spark.table("workspace.bronze.itens_raw")
bronze_resultados = spark.table("workspace.bronze.resultados_raw")
bronze_contratacoes = spark.table("workspace.bronze.contratacoes_raw")

itens, itens_quarentena, itens_conflitos = transformar_itens(bronze_itens)
itens = classificar_equipamentos(itens)
resultados, fornecedores, resultados_quarentena, resultados_conflitos = (
    transformar_resultados(
        bronze_resultados,
        dbutils.secrets.get(scope="rastro-publico", key="fornecedor-hmac"),
    )
)
orgaos, unidades = transformar_dimensoes(bronze_contratacoes)
vinculos = transformar_vinculos_contratacao(bronze_contratacoes)

contratacoes = spark.table("workspace.silver.contratacoes")
itens_sem_contratacao = itens.join(
    contratacoes.select("contratacao_id"), "contratacao_id", "left_anti"
).withColumn("motivo_quarentena", lit("contratacao_nao_carregada"))
itens = itens.join(contratacoes.select("contratacao_id"), "contratacao_id", "inner")
resultados_sem_item = resultados.join(
    itens.select("item_id"), "item_id", "left_anti"
).withColumn("motivo_quarentena", lit("item_nao_carregado"))
resultados = resultados.join(itens.select("item_id"), "item_id", "inner")
fornecedores = fornecedores.join(
    resultados.select("fornecedor_id").distinct(), "fornecedor_id", "inner"
)

materializar_snapshot(itens, "workspace.silver.itens_contratacao")
materializar_snapshot(resultados, "workspace.silver.resultados_itens")
materializar_snapshot(fornecedores, "workspace.silver.fornecedores")
materializar_snapshot(orgaos, "workspace.silver.orgaos")
materializar_snapshot(unidades, "workspace.silver.unidades_compradoras")
materializar_snapshot(vinculos, "workspace.silver.contratacoes_dimensoes")

problemas = (
    itens_quarentena.withColumn("entidade", lit("item"))
    .unionByName(
        itens_conflitos.withColumn("entidade", lit("item")).withColumn(
            "motivo_quarentena", lit("empate_conteudo_divergente")
        ),
        allowMissingColumns=True,
    )
    .unionByName(
        resultados_quarentena.withColumn("entidade", lit("resultado")),
        allowMissingColumns=True,
    )
    .unionByName(
        resultados_conflitos.withColumn("entidade", lit("resultado")).withColumn(
            "motivo_quarentena", lit("empate_conteudo_divergente")
        ),
        allowMissingColumns=True,
    )
    .unionByName(
        itens_sem_contratacao.withColumn("entidade", lit("item")),
        allowMissingColumns=True,
    )
    .unionByName(
        resultados_sem_item.withColumn("entidade", lit("resultado")),
        allowMissingColumns=True,
    )
    .withColumn(
        "registro_problema_id",
        sha2(
            concat_ws(
                "|",
                "entidade",
                "source_file_id",
                "id_origem_item",
                "id_origem_resultado",
                "motivo_quarentena",
                "hash_conteudo_entidade",
            ),
            256,
        ),
    )
)
tabela_problemas = "workspace.silver.nucleo_problemas"
materializar_snapshot(problemas, tabela_problemas)

resultado = {
    "bronze_itens": bronze_itens.count(),
    "bronze_resultados": bronze_resultados.count(),
    "silver_itens": spark.table("workspace.silver.itens_contratacao").count(),
    "silver_resultados": spark.table("workspace.silver.resultados_itens").count(),
    "fornecedores": spark.table("workspace.silver.fornecedores").count(),
    "orgaos": spark.table("workspace.silver.orgaos").count(),
    "unidades": spark.table("workspace.silver.unidades_compradoras").count(),
    "problemas": spark.table(tabela_problemas).count(),
    "resultados_sem_item": resultados_sem_item.count(),
    "itens_sem_contratacao": itens_sem_contratacao.count(),
}
print(json.dumps(resultado))
