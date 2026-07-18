# Databricks notebook source
# ruff: noqa: E402, F821
import json
import sys
from datetime import date, datetime, timezone

from pyspark.sql.functions import col, concat_ws, lit, sha2


dbutils.widgets.text("source_root", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.dropdown(
    "modo", "reprocessamento", ["bootstrap", "incremental", "reprocessamento"]
)
dbutils.widgets.text("data_inicio", "")
dbutils.widgets.text("data_fim", "")
dbutils.widgets.text("sobreposicao_dias", "3")
source_root = dbutils.widgets.get("source_root")
run_id = dbutils.widgets.get("run_id")
modo = dbutils.widgets.get("modo")
data_inicio_param = dbutils.widgets.get("data_inicio")
data_fim_param = dbutils.widgets.get("data_fim")
sobreposicao_dias = int(dbutils.widgets.get("sobreposicao_dias"))
if not source_root or not run_id or not data_fim_param:
    raise ValueError("source_root, run_id e data_fim sao obrigatorios")
sys.path.insert(0, source_root)

from rastro_publico.operacao import avaliar_regra, decidir_watermark, janela_incremental
from rastro_publico.transformacoes.nucleo import (
    classificar_equipamentos,
    transformar_dimensoes,
    transformar_itens,
    transformar_resultados,
    transformar_vinculos_contratacao,
)


pipeline_id = "nucleo_compras"
recorte_id = "brasil_tecnologia"
data_fim = date.fromisoformat(data_fim_param)
data_inicio = date.fromisoformat(data_inicio_param) if data_inicio_param else data_fim
if data_inicio > data_fim:
    raise ValueError("data_inicio posterior a data_fim")

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ops")
tabela_estado = "workspace.ops.pipeline_state"
estado_anterior = None
versao_estado = 0
if spark.catalog.tableExists(tabela_estado):
    linha_estado = (
        spark.table(tabela_estado)
        .where((col("pipeline_id") == pipeline_id) & (col("recorte_id") == recorte_id))
        .first()
    )
    if linha_estado:
        estado_anterior = linha_estado.watermark_concluido
        versao_estado = linha_estado.versao_estado

if modo == "incremental":
    if estado_anterior is None:
        raise ValueError("incremental exige watermark de bootstrap")
    data_inicio, data_fim = janela_incremental(
        estado_anterior, data_fim, sobreposicao_dias
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

# ponytail: reconstrução integral é determinística no volume atual; migrar para
# MERGE por chaves afetadas somente se o benchmark do Bloco 7 justificar.
metricas = [
    avaliar_regra(
        "itens_duplicados",
        itens.groupBy("item_id").count().where("count > 1").count(),
        itens.count(),
        0,
        "erro",
    ),
    avaliar_regra(
        "resultados_duplicados",
        resultados.groupBy("resultado_id").count().where("count > 1").count(),
        resultados.count(),
        0,
        "erro",
    ),
    avaliar_regra(
        "cpf_publico",
        fornecedores.where(
            (col("tipo_pessoa") == "PF") & col("identificador_publico").isNotNull()
        ).count(),
        fornecedores.count(),
        0,
        "erro",
    ),
    avaliar_regra(
        "itens_sem_contratacao_publicada",
        itens.join(contratacoes, "contratacao_id", "left_anti").count(),
        itens.count(),
        0,
        "erro",
    ),
    avaliar_regra(
        "resultados_sem_item_publicado",
        resultados.join(itens, "item_id", "left_anti").count(),
        resultados.count(),
        0,
        "erro",
    ),
    avaliar_regra(
        "vinculos_pendentes",
        itens_sem_contratacao.count() + resultados_sem_item.count(),
        bronze_itens.count() + bronze_resultados.count(),
        0,
        "alerta",
    ),
    avaliar_regra(
        "registros_em_quarentena",
        itens_quarentena.count() + resultados_quarentena.count(),
        bronze_itens.count() + bronze_resultados.count(),
        0,
        "alerta",
    ),
    avaliar_regra(
        "conflitos_de_versao",
        itens_conflitos.count() + resultados_conflitos.count(),
        bronze_itens.count() + bronze_resultados.count(),
        0,
        "alerta",
    ),
]
registrado_em = datetime.now(timezone.utc)
linhas_qualidade = [
    {
        **metrica,
        "run_id": run_id,
        "pipeline_id": pipeline_id,
        "recorte_id": recorte_id,
        "registrado_em": registrado_em,
    }
    for metrica in metricas
]
tabela_qualidade = "workspace.ops.quality_results"
spark.createDataFrame(linhas_qualidade).write.format("delta").mode("append").option(
    "mergeSchema", "true"
).saveAsTable(tabela_qualidade)

falhas = [metrica["regra"] for metrica in metricas if metrica["status"] == "FAIL"]
if falhas:
    raise RuntimeError(f"qualidade bloqueante falhou: {', '.join(falhas)}")

novo_watermark = decidir_watermark(estado_anterior, data_fim, modo, True)
if novo_watermark is not None:
    novo_estado = spark.createDataFrame(
        [
            {
                "pipeline_id": pipeline_id,
                "recorte_id": recorte_id,
                "watermark_concluido": novo_watermark,
                "sobreposicao_dias": sobreposicao_dias,
                "ultimo_run_id": run_id,
                "atualizado_em": registrado_em,
                "versao_estado": versao_estado + 1,
            }
        ]
    )
    novo_estado.createOrReplaceTempView("novo_pipeline_state")
    spark.sql(
        f"""
        MERGE INTO {tabela_estado} AS t
        USING novo_pipeline_state AS s
        ON t.pipeline_id = s.pipeline_id AND t.recorte_id = s.recorte_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        if spark.catalog.tableExists(tabela_estado)
        else f"CREATE TABLE {tabela_estado} USING DELTA AS SELECT * FROM novo_pipeline_state"
    )

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
    "modo": modo,
    "data_inicio_efetiva": data_inicio.isoformat(),
    "data_fim": data_fim.isoformat(),
    "watermark_anterior": estado_anterior.isoformat() if estado_anterior else None,
    "watermark_novo": novo_watermark.isoformat() if novo_watermark else None,
    "qualidade": metricas,
}
print(json.dumps(resultado))
