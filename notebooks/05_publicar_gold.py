# Databricks notebook source
# ruff: noqa: E402, F821
import json
import math
import sys


dbutils.widgets.text("source_root", "")
dbutils.widgets.text("minimo_fornecedores", "2")
dbutils.widgets.text("minimo_resultados", "3")
dbutils.widgets.text("cobertura_minima", "0.8")
source_root = dbutils.widgets.get("source_root")
if not source_root:
    raise ValueError("source_root e obrigatorio")
sys.path.insert(0, source_root)

from rastro_publico.transformacoes.gold import (
    calcular_concentracao_fornecedores,
    calcular_cobertura_servicos,
    calcular_qualidade_cobertura,
)


minimo_fornecedores = int(dbutils.widgets.get("minimo_fornecedores"))
minimo_resultados = int(dbutils.widgets.get("minimo_resultados"))
cobertura_minima = float(dbutils.widgets.get("cobertura_minima"))

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
)
servicos = calcular_cobertura_servicos(itens)

violacoes = concentracao.where(
    "top_1 < 0 OR top_1 > top_3 OR top_3 > 1 OR hhi < 0 OR hhi > 1 "
    "OR cobertura_valor < 0 OR cobertura_valor > 1"
).count()
if violacoes:
    raise RuntimeError(f"invariantes de concentracao violadas: {violacoes}")

concentracao.createOrReplaceTempView("gold_concentracao_pyspark")
itens.createOrReplaceTempView("gold_itens_entrada")
resultados.createOrReplaceTempView("gold_resultados_entrada")
contratacoes.createOrReplaceTempView("gold_contratacoes_entrada")
vinculos.createOrReplaceTempView("gold_vinculos_entrada")

equivalencia_sql = (
    spark.sql(
        """
    WITH base AS (
      SELECT date_format(to_timestamp(c.publicado_em), 'yyyy-MM') AS periodo,
             v.orgao_id, i.categoria_tecnologia, c.modalidade_id, c.modalidade,
             r.resultado_id, r.fornecedor_id, r.valor_total_homologado
      FROM gold_resultados_entrada r
      JOIN gold_itens_entrada i USING (item_id)
      JOIN gold_contratacoes_entrada c USING (contratacao_id)
      JOIN gold_vinculos_entrada v USING (contratacao_id)
      WHERE i.categoria_tecnologia <> 'incerto'
        AND c.publicado_em IS NOT NULL
        AND c.modalidade_id IS NOT NULL
        AND v.orgao_id IS NOT NULL
        AND NOT r.cancelado
        AND r.fornecedor_id IS NOT NULL
        AND r.valor_total_homologado > 0
    ), fornecedor AS (
      SELECT periodo, orgao_id, categoria_tecnologia, modalidade_id, modalidade,
             fornecedor_id, SUM(valor_total_homologado) AS valor_fornecedor,
             COUNT(DISTINCT resultado_id) AS resultados_fornecedor
      FROM base
      GROUP BY ALL
    ), participacao AS (
      SELECT *,
             SUM(valor_fornecedor) OVER (PARTITION BY periodo, orgao_id,
               categoria_tecnologia, modalidade_id, modalidade) AS valor_total,
             valor_fornecedor / SUM(valor_fornecedor) OVER (PARTITION BY periodo,
               orgao_id, categoria_tecnologia, modalidade_id, modalidade) AS parcela,
             ROW_NUMBER() OVER (PARTITION BY periodo, orgao_id, categoria_tecnologia,
               modalidade_id, modalidade ORDER BY valor_fornecedor DESC, fornecedor_id) AS posicao
      FROM fornecedor
    ), sql_final AS (
      SELECT periodo, orgao_id, categoria_tecnologia, modalidade_id, modalidade,
             MAX(valor_total) AS valor_total_homologado,
             COUNT(*) AS fornecedores_distintos,
             SUM(resultados_fornecedor) AS resultados_elegiveis,
             MAX(parcela) AS top_1,
             LEAST(1.0, SUM(CASE WHEN posicao <= 3 THEN parcela ELSE 0 END)) AS top_3,
             SUM(parcela * parcela) AS hhi
      FROM participacao
      GROUP BY ALL
    )
    , pyspark_final AS (
      SELECT periodo, orgao_id, categoria_tecnologia, modalidade_id, modalidade,
             CAST(valor_total_homologado AS DECIMAL(38,2)) AS valor_total_homologado,
             fornecedores_distintos, resultados_elegiveis,
             ROUND(top_1, 10) AS top_1, ROUND(top_3, 10) AS top_3, ROUND(hhi, 10) AS hhi
      FROM gold_concentracao_pyspark
    ), sql_normalizado AS (
      SELECT periodo, orgao_id, categoria_tecnologia, modalidade_id, modalidade,
             CAST(valor_total_homologado AS DECIMAL(38,2)) AS valor_total_homologado,
             fornecedores_distintos, resultados_elegiveis,
             ROUND(top_1, 10) AS top_1, ROUND(top_3, 10) AS top_3, ROUND(hhi, 10) AS hhi
      FROM sql_final
    )
    SELECT COUNT(*) AS divergencias
    FROM (
      SELECT * FROM (
        SELECT * FROM sql_normalizado
        EXCEPT ALL
        SELECT * FROM pyspark_final
      )
      UNION ALL
      SELECT * FROM (
        SELECT * FROM pyspark_final
        EXCEPT ALL
        SELECT * FROM sql_normalizado
      )
    )
    """
    )
    .first()
    .divergencias
)
if equivalencia_sql:
    raise RuntimeError(f"PySpark e Spark SQL divergiram em {equivalencia_sql} grupos")

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

resumo = concentracao.agg(
    {"valor_total_homologado": "sum", "resultados_elegiveis": "sum"}
).first()
total_gold = float(resumo[0] or 0)
if not math.isfinite(total_gold):
    raise RuntimeError("valor Gold nao finito")

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
            "valor_total_gold": total_gold,
            "equivalencia_sql_divergencias": equivalencia_sql,
            "invariantes_violadas": violacoes,
            "minimo_fornecedores": minimo_fornecedores,
            "minimo_resultados": minimo_resultados,
            "cobertura_minima": cobertura_minima,
        }
    )
)
