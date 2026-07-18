# Databricks notebook source
# ruff: noqa: E402, F821
import json
import sys

from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    max as spark_max,
    sha2,
    struct,
    to_json,
)


dbutils.widgets.text("source_root", "")
dbutils.widgets.text("landing_root", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("competencia", "")
source_root = dbutils.widgets.get("source_root")
landing_root = dbutils.widgets.get("landing_root")
run_id = dbutils.widgets.get("run_id")
competencia = dbutils.widgets.get("competencia")
if not all((source_root, landing_root, run_id, competencia)):
    raise ValueError("source_root, landing_root, run_id e competencia sao obrigatorios")
sys.path.insert(0, source_root)

from rastro_publico.transformacoes.contexto import (
    enriquecer_fornecedores_contexto,
    resumir_qsa,
    transformar_empresas_cnpj,
    transformar_sancoes,
)
from rastro_publico.transformacoes.contratos import (
    filtrar_populacao_contratual_tecnologia,
)
from rastro_publico.transformacoes.gold import (
    calcular_evolucao_contratual,
    calcular_presenca_fornecedores,
    calcular_recorrencia_orgao_fornecedor,
    calcular_rede_orgao_fornecedor,
    calcular_variacao_precos,
)


def publicar_bronze(dados, tabela, dataset):
    colunas_origem = sorted(dados.columns)
    origem = (
        dados.withColumn("dataset_origem", lit(dataset))
        .withColumn("competencia", lit(competencia))
        .withColumn("run_id", lit(run_id))
        .withColumn("coletado_em_utc", current_timestamp())
        .withColumn("hash_payload", sha2(to_json(struct(*colunas_origem)), 256))
        .dropDuplicates(["dataset_origem", "competencia", "hash_payload"])
    )
    if not spark.catalog.tableExists(tabela):
        origem.write.format("delta").saveAsTable(tabela)
        return origem.count()
    delta = DeltaTable.forName(spark, tabela)
    (
        delta.alias("destino")
        .merge(
            origem.alias("origem"),
            "destino.dataset_origem = origem.dataset_origem "
            "AND destino.competencia = origem.competencia "
            "AND destino.hash_payload = origem.hash_payload",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    return origem.count()


empresas_raw = spark.read.option("header", True).csv(
    f"{landing_root}/empresas_fornecedores.csv"
)
qsa_raw = spark.read.option("header", True).csv(
    f"{landing_root}/socios_fornecedores_minimizado.csv"
)
ceis_raw = spark.read.option("header", True).csv(
    f"{landing_root}/ceis_fornecedores_minimizado.csv"
)
cnep_raw = spark.read.option("header", True).csv(
    f"{landing_root}/cnep_fornecedores_minimizado.csv"
)
municipios_raw = spark.read.option("multiline", True).json(
    f"{landing_root}/municipios_ibge.json"
)
ipca_raw = spark.read.option("multiline", True).json(
    f"{landing_root}/ipca_indice.json"
)
sancoes_raw = ceis_raw.unionByName(cnep_raw)

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
bronze_linhas = {
    "empresas": publicar_bronze(
        empresas_raw, "workspace.bronze.cnpj_empresas_raw", "cnpj_empresas"
    ),
    "qsa": publicar_bronze(qsa_raw, "workspace.bronze.cnpj_qsa_raw", "cnpj_qsa"),
    "sancoes": publicar_bronze(
        sancoes_raw, "workspace.bronze.sancoes_raw", "ceis_cnep"
    ),
    "municipios": publicar_bronze(
        municipios_raw, "workspace.bronze.municipios_ibge_raw", "municipios_ibge"
    ),
    "ipca": publicar_bronze(ipca_raw, "workspace.bronze.ipca_raw", "ipca_indice"
    ),
}

empresas = transformar_empresas_cnpj(empresas_raw).withColumn(
    "competencia", lit(competencia)
)
qsa = resumir_qsa(qsa_raw).withColumn("competencia", lit(competencia))
sancoes = transformar_sancoes(sancoes_raw).withColumn(
    "competencia", lit(competencia)
)
municipios = municipios_raw.select(
    col("id").cast("string").alias("codigo_ibge"),
    col("nome").alias("municipio"),
    col("microrregiao.mesorregiao.UF.sigla").alias("uf"),
    col("microrregiao.mesorregiao.UF.nome").alias("nome_uf"),
    col("microrregiao.mesorregiao.UF.regiao.sigla").alias("regiao"),
).withColumn("competencia", lit(competencia))
ipca = (
    ipca_raw.where(col("D3C").rlike(r"^\d{6}$"))
    .select(
        col("D3C").alias("periodo"),
        col("D3N").alias("mes"),
        col("V").cast("decimal(18,6)").alias("numero_indice"),
    )
    .withColumn("competencia", lit(competencia))
)

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
for dados, tabela in (
    (empresas, "workspace.silver.cnpj_empresas_contexto"),
    (qsa, "workspace.silver.cnpj_qsa_resumo"),
    (sancoes, "workspace.silver.sancoes_fornecedores_contexto"),
    (municipios, "workspace.silver.municipios_ibge"),
    (ipca, "workspace.silver.ipca"),
):
    dados.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(tabela)

itens = spark.table("workspace.silver.itens_contratacao")
resultados = spark.table("workspace.silver.resultados_itens")
contratacoes = spark.table("workspace.silver.contratacoes")
vinculos = spark.table("workspace.silver.contratacoes_dimensoes")
unidades = spark.table("workspace.silver.unidades_compradoras")
contratos = spark.table("workspace.silver.contratos")
itens_contrato = spark.table("workspace.silver.itens_contrato")
eventos = spark.table("workspace.silver.eventos_contrato")
fornecedores_contratos = spark.table("workspace.silver.fornecedores_contratos")
contratos_tecnologia, eventos_tecnologia, fornecedores_contratos_tecnologia = (
    filtrar_populacao_contratual_tecnologia(
        contratos,
        itens_contrato,
        eventos,
        fornecedores_contratos,
    )
)
fornecedores = (
    spark.table("workspace.silver.fornecedores")
    .select("fornecedor_id", "identificador_publico", "nome_fornecedor")
    .unionByName(
        fornecedores_contratos_tecnologia.select(
            "fornecedor_id", "identificador_publico", "nome_fornecedor"
        )
    )
    .dropDuplicates(["fornecedor_id"])
)

gold = {
    "recorrencia_orgao_fornecedor": calcular_recorrencia_orgao_fornecedor(
        itens, resultados, contratacoes, vinculos
    ),
    "presenca_fornecedores": calcular_presenca_fornecedores(
        itens, resultados, contratacoes, vinculos, unidades
    ),
    "variacao_precos": calcular_variacao_precos(itens, resultados, contratacoes),
    "evolucao_contratual": calcular_evolucao_contratual(
        contratos_tecnologia, eventos_tecnologia
    ),
    "arestas_orgao_fornecedor": calcular_rede_orgao_fornecedor(
        itens, resultados, contratacoes, vinculos
    ),
    "fornecedores_contexto": enriquecer_fornecedores_contexto(
        fornecedores,
        empresas.drop("competencia"),
        qsa.drop("competencia"),
        sancoes.drop("competencia"),
    ).withColumn("competencia", lit(competencia)),
}
indice_referencia = ipca.agg(spark_max("numero_indice")).first()[0]
gold["ipca_referencia"] = ipca.withColumn(
    "fator_para_referencia", lit(indice_referencia) / col("numero_indice")
).withColumn("periodo_referencia", lit("202606"))

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")
for nome, dados in gold.items():
    dados.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"workspace.gold.{nome}")

if gold["variacao_precos"].where("status_publicacao = 'publicada'").count():
    raise RuntimeError("precos foram publicados sem grupo comparavel defensavel")
if gold["fornecedores_contexto"].where("interpretacao LIKE '%fraude%'").count():
    raise RuntimeError("linguagem proibida detectada no contexto")

print(
    json.dumps(
        {
            "bronze_linhas_entrada": bronze_linhas,
            "silver": {
                "empresas": empresas.count(),
                "qsa": qsa.count(),
                "sancoes": sancoes.count(),
                "municipios": municipios.count(),
                "ipca": ipca.count(),
            },
            "gold": {nome: dados.count() for nome, dados in gold.items()},
            "precos_publicados": 0,
            "periodo_ipca_referencia": "202606",
        },
        ensure_ascii=False,
    )
)
