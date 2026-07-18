# Databricks notebook source
# ruff: noqa: F821
import json

from pyspark.sql.functions import lit, to_date, to_timestamp, trim


dbutils.widgets.text("landing_root", "")
dbutils.widgets.text("data_inicio", "")
dbutils.widgets.text("data_fim", "")
dbutils.widgets.text("run_id", "")
landing_root = dbutils.widgets.get("landing_root")
data_inicio = dbutils.widgets.get("data_inicio")
data_fim = dbutils.widgets.get("data_fim")
run_id = dbutils.widgets.get("run_id")
if not all((landing_root, data_inicio, data_fim, run_id)):
    raise ValueError("landing_root, data_inicio, data_fim e run_id sao obrigatorios")


def arquivos(dataset):
    return sorted(
        item.path
        for item in dbutils.fs.ls(landing_root)
        if item.name.endswith(".csv") and f"-{dataset}-" in item.name
    )


def ler_dataset(dataset, sistema):
    partes = []
    for caminho in arquivos(dataset):
        manifesto = json.loads(dbutils.fs.head(f"{caminho}.manifest.json"))
        parte = (
            spark.read.option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .csv(caminho)
            .withColumn("_source_file_id", lit(manifesto["hash_arquivo"]))
            .withColumn("_run_id", lit(run_id))
            .withColumn("_sistema_origem", lit(sistema))
            .withColumn("_dataset_origem", lit(dataset))
            .withColumn("_coletado_em_utc", lit(manifesto["coletado_em_utc"]))
            .withColumn(
                "_data_publicacao_arquivo",
                lit(manifesto.get("data_publicacao_arquivo")),
            )
        )
        partes.append(parte)
    if not partes:
        raise ValueError(f"nenhum arquivo para {dataset}")
    resultado = partes[0]
    for parte in partes[1:]:
        resultado = resultado.unionByName(parte, allowMissingColumns=True)
    return resultado


compras_todas = ler_dataset("VW_FT_PNCP_COMPRA", "comprasgov")
itens_todos = ler_dataset("VW_FT_PNCP_COMPRA_ITEM", "comprasgov")
resultados_todos = ler_dataset("VW_DM_PNCP_ITEM_RESULTADO", "comprasgov")
contratos_todos = ler_dataset("anual-contratos", "comprasnet_contratos")
contratos_itens_todos = ler_dataset("anual-itens", "comprasnet_contratos")
historicos_todos = ler_dataset("anual-historicos", "comprasnet_contratos")

compras = compras_todas.where(
    to_date(to_timestamp("data_publicacao_pncp")).between(data_inicio, data_fim)
)
chaves_compras = compras.select(
    trim("numero_controle_PNCP").alias("numero_controle_pncp")
).where("numero_controle_pncp IS NOT NULL").distinct()
itens = (
    itens_todos.withColumn(
        "numero_controle_pncp", trim("numero_controle_PNCP_compra")
    )
    .join(chaves_compras, "numero_controle_pncp", "left_semi")
    .drop("numero_controle_pncp")
)
resultados = (
    resultados_todos.withColumn(
        "numero_controle_pncp", trim("numero_controle_PNCP_compra")
    )
    .join(chaves_compras, "numero_controle_pncp", "left_semi")
    .drop("numero_controle_pncp")
)

contratos = contratos_todos.where(
    to_date(to_timestamp("data_publicacao")).between(data_inicio, data_fim)
)
chaves_contratos = contratos.select(trim("id").alias("id_contrato_origem")).where(
    "id_contrato_origem IS NOT NULL"
).distinct()
contratos_itens = (
    contratos_itens_todos.withColumn(
        "id_contrato_origem", trim("contrato_id")
    )
    .join(chaves_contratos, "id_contrato_origem", "left_semi")
    .drop("id_contrato_origem")
)
historicos = (
    historicos_todos.withColumn("id_contrato_origem", trim("contrato_id"))
    .join(chaves_contratos, "id_contrato_origem", "left_semi")
    .drop("id_contrato_origem")
)

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.staging")
tabelas = {
    "contratacoes": (compras, "workspace.staging.contratacoes_raw"),
    "itens": (itens, "workspace.staging.itens_raw"),
    "resultados": (resultados, "workspace.staging.resultados_raw"),
    "contratos": (contratos, "workspace.staging.contratos_raw"),
    "contratos_itens": (contratos_itens, "workspace.staging.contrato_itens_raw"),
    "historicos": (historicos, "workspace.staging.contrato_historicos_raw"),
}
for dados, tabela in tabelas.values():
    dados.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(tabela)

contagens = {nome: spark.table(tabela).count() for nome, (_, tabela) in tabelas.items()}
if not all(contagens.values()):
    raise RuntimeError(f"entidade vazia na janela: {contagens}")

print(
    json.dumps(
        {
            "run_id": run_id,
            "data_inicio": data_inicio,
            "data_fim": data_fim,
            "fontes": {
                "compras": len(arquivos("VW_FT_PNCP_COMPRA")),
                "itens": len(arquivos("VW_FT_PNCP_COMPRA_ITEM")),
                "resultados": len(arquivos("VW_DM_PNCP_ITEM_RESULTADO")),
                "contratos": len(arquivos("anual-contratos")),
                "contratos_itens": len(arquivos("anual-itens")),
                "historicos": len(arquivos("anual-historicos")),
            },
            "bronze": contagens,
            "observacao_cobertura": (
                "snapshots anuais oficiais coletados em 2026-07-18; "
                "datas efetivas verificadas nas tabelas de saída"
            ),
        },
        ensure_ascii=False,
    )
)
