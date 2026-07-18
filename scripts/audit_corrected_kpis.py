"""Recalcula KPIs de portfólio diretamente dos snapshots anuais."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    lower,
    regexp_replace,
    to_date,
    to_timestamp,
    trim,
    when,
)

from rastro_publico.transformacoes.nucleo import (
    classificar_equipamentos,
    classificar_servicos,
)


def _files(root: Path, pattern: str) -> list[str]:
    files = sorted(root.glob(pattern))
    if not files:
        raise FileNotFoundError(pattern)
    return [file.resolve().as_uri() for file in files]


def _read(spark: SparkSession, paths: list[str]) -> DataFrame:
    parts = [
        spark.read.options(header=True, multiLine=True, escape='"').csv(path)
        for path in paths
    ]
    result = parts[0]
    for part in parts[1:]:
        result = result.unionByName(part, allowMissingColumns=True)
    return result


def _classify(items: DataFrame, description: str, item_type: str) -> DataFrame:
    prepared = items.withColumn("descricao", trim(col(description))).withColumn(
        "material_ou_servico", col(item_type)
    )
    return classificar_servicos(classificar_equipamentos(prepared))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--data-inicio", required=True)
    parser.add_argument("--data-fim", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.master("local[4]")
        .appName("rastro-publico-kpis-corrigidos")
        .config("spark.sql.shuffle.partitions", "24")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    purchases = _read(spark, _files(args.root, "*VW_FT_PNCP_COMPRA-*.csv")).where(
        to_date(to_timestamp("data_publicacao_pncp")).between(
            args.data_inicio, args.data_fim
        )
    )
    purchase_population = purchases.select(
        trim("numero_controle_PNCP").alias("contratacao_id"),
        trim("orgao_entidade_cnpj").alias("orgao_id"),
    ).where("contratacao_id IS NOT NULL")

    purchase_items = _read(
        spark, _files(args.root, "*VW_FT_PNCP_COMPRA_ITEM-*.csv")
    )
    technology_items = (
        _classify(
            purchase_items,
            "descricao_resumida",
            "material_ou_servico",
        )
        .select(
            trim("numero_controle_PNCP_compra").alias("contratacao_id"),
            trim("id_compra_item").alias("item_id"),
            "categoria_tecnologia",
        )
        .where("categoria_tecnologia <> 'incerto'")
        .join(purchase_population, "contratacao_id", "left_semi")
        .dropDuplicates(["item_id", "categoria_tecnologia"])
        .persist(StorageLevel.DISK_ONLY)
    )

    results = _read(
        spark, _files(args.root, "*VW_DM_PNCP_ITEM_RESULTADO-*.csv")
    ).select(
        trim("id_compra_item").alias("item_id"),
        trim("numero_controle_PNCP_compra").alias("contratacao_id"),
        trim("ni_fornecedor").alias("fornecedor_id"),
    )
    technology_results = (
        results.join(
            technology_items.select("item_id", "categoria_tecnologia"),
            "item_id",
            "inner",
        )
        .join(purchase_population, "contratacao_id", "inner")
        .where("fornecedor_id IS NOT NULL AND fornecedor_id <> ''")
        .dropDuplicates(
            ["contratacao_id", "item_id", "fornecedor_id", "categoria_tecnologia"]
        )
        .persist(StorageLevel.DISK_ONLY)
    )
    recurrence = (
        technology_results.select(
            "contratacao_id", "orgao_id", "fornecedor_id", "categoria_tecnologia"
        )
        .distinct()
        .groupBy("orgao_id", "fornecedor_id", "categoria_tecnologia")
        .agg(countDistinct("contratacao_id").alias("contratacoes_distintas"))
        .where("contratacoes_distintas >= 2")
    )

    contracts = _read(
        spark, _files(args.root, "*contratos-anual-contratos-*.csv")
    ).where(
        to_date(to_timestamp("data_publicacao")).between(
            args.data_inicio, args.data_fim
        )
    )
    contract_population = contracts.select(
        trim("id").alias("contrato_id"),
        trim("fonecedor_cnpj_cpf_idgener").alias("fornecedor_id"),
    ).where("contrato_id IS NOT NULL")
    contract_items = _read(
        spark, _files(args.root, "*contratos-anual-itens-*.csv")
    )
    contract_items = contract_items.withColumn(
        "tipo_normalizado",
        when(lower(col("tipo_id")).contains("serv"), "S")
        .when(lower(col("tipo_id")).contains("material"), "M")
    )
    # O arquivo anual usa descrições textuais ou códigos; a regra conservadora
    # só promove serviço quando o campo o identifica explicitamente.
    technology_contract_ids = (
        _classify(
            contract_items,
            "descricao_complementar",
            "tipo_normalizado",
        )
        .where("categoria_tecnologia <> 'incerto'")
        .select(trim("contrato_id").alias("contrato_id"))
        .join(contract_population.select("contrato_id"), "contrato_id", "left_semi")
        .distinct()
        .persist(StorageLevel.DISK_ONLY)
    )
    technology_contracts = contract_population.join(
        technology_contract_ids, "contrato_id", "inner"
    )
    histories = _read(
        spark, _files(args.root, "*contratos-anual-historicos-*.csv")
    ).select(
        trim("id").alias("evento_id"), trim("contrato_id").alias("contrato_id")
    )
    technology_events = histories.join(
        technology_contract_ids, "contrato_id", "inner"
    )
    purchase_suppliers = technology_results.select(
        regexp_replace("fornecedor_id", r"\D", "").alias("fornecedor_id")
    ).where("fornecedor_id <> ''")
    contract_suppliers = technology_contracts.select(
        regexp_replace("fornecedor_id", r"\D", "").alias("fornecedor_id")
    ).where("fornecedor_id <> ''")

    categories = {
        row["categoria_tecnologia"]: row["count"]
        for row in technology_items.groupBy("categoria_tecnologia").count().collect()
    }
    output = {
        "janela": {"inicio": args.data_inicio, "fim": args.data_fim},
        "compras_tecnologia": technology_items.select("contratacao_id").distinct().count(),
        "itens_tecnologia": technology_items.select("item_id").distinct().count(),
        "resultados_tecnologia": technology_results.count(),
        "fornecedores_compras_tecnologia": technology_results.select(
            "fornecedor_id"
        ).distinct().count(),
        "relacoes_recorrentes": recurrence.count(),
        "contratos_tecnologia": technology_contracts.select("contrato_id").distinct().count(),
        "fornecedores_contratos_tecnologia": technology_contracts.where(
            "fornecedor_id IS NOT NULL AND fornecedor_id <> ''"
        ).select("fornecedor_id").distinct().count(),
        "fornecedores_tecnologia_distintos": purchase_suppliers.unionByName(
            contract_suppliers
        ).distinct().count(),
        "eventos_contratos_tecnologia": technology_events.select(
            "evento_id"
        ).distinct().count(),
        "itens_por_categoria": dict(sorted(categories.items())),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
