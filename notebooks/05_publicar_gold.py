# Databricks notebook source
# ruff: noqa: E402, F821
import json
import sys


dbutils.widgets.text("source_root", "")
dbutils.widgets.text("minimo_fornecedores", "2")
dbutils.widgets.text("minimo_resultados", "3")
dbutils.widgets.text("cobertura_minima", "0.8")
dbutils.widgets.dropdown("semantica_fonte_validada", "false", ["false", "true"])
source_root = dbutils.widgets.get("source_root")
if not source_root:
    raise ValueError("source_root e obrigatorio")
sys.path.insert(0, source_root)

from rastro_publico.transformacoes.gold import (
    calcular_concentracao_fornecedores,
    calcular_cobertura_servicos,
    calcular_kpis_compras,
    calcular_qualidade_cobertura,
)


minimo_fornecedores = int(dbutils.widgets.get("minimo_fornecedores"))
minimo_resultados = int(dbutils.widgets.get("minimo_resultados"))
cobertura_minima = float(dbutils.widgets.get("cobertura_minima"))
semantica_fonte_validada = dbutils.widgets.get("semantica_fonte_validada") == "true"

itens = spark.table("workspace.silver.itens_contratacao")
resultados = spark.table("workspace.silver.resultados_itens")
contratacoes = spark.table("workspace.silver.contratacoes")
vinculos = spark.table("workspace.silver.contratacoes_dimensoes")
qualidade_operacional = spark.table("workspace.ops.quality_results")

qualidade = calcular_qualidade_cobertura(
    itens, resultados, contratacoes, vinculos, qualidade_operacional
)
concentracao = calcular_concentracao_fornecedores(
    itens,
    resultados,
    contratacoes,
    vinculos,
    minimo_fornecedores=minimo_fornecedores,
    minimo_resultados=minimo_resultados,
    cobertura_minima=cobertura_minima,
    semantica_fonte_validada=semantica_fonte_validada,
)
servicos = calcular_cobertura_servicos(itens)
kpis_compras = calcular_kpis_compras(itens, resultados, contratacoes, vinculos)

violacoes = concentracao.where(
    "top_1 < 0 OR top_1 > top_3 OR top_3 > 1 OR hhi < 0 OR hhi > 1 "
    "OR cobertura_valor < 0 OR cobertura_valor > 1"
).count()
if violacoes:
    raise RuntimeError(f"invariantes de concentracao violadas: {violacoes}")

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")
# ponytail: overwrite atomico basta enquanto a Gold cobre uma unica janela pequena;
# recalculo por periodos afetados entra somente quando o Bloco 11 medir necessidade.
qualidade.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("workspace.gold.qualidade_cobertura")
concentracao.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("workspace.gold.concentracao_fornecedores")
servicos.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("workspace.gold.servicos_cobertura")

print(
    json.dumps(
        {
            "qualidade_linhas": qualidade.count(),
            "concentracao_grupos": concentracao.count(),
            "concentracao_publicados": concentracao.where(
                "status_publicacao = 'publicada'"
            ).count(),
            "concentracao_nao_publicaveis": concentracao.where(
                "status_publicacao = 'nao_publicavel'"
            ).count(),
            "servicos_categorias": servicos.count(),
            "servicos_precos_publicados": servicos.where(
                "status_publicacao_preco = 'publicada'"
            ).count(),
            "invariantes_violadas": violacoes,
            "semantica_fonte_validada": semantica_fonte_validada,
            "kpis_compras": kpis_compras.first().asDict(),
            "minimo_fornecedores": minimo_fornecedores,
            "minimo_resultados": minimo_resultados,
            "cobertura_minima": cobertura_minima,
        }
    )
)
