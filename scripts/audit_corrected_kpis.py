"""Reproduz os KPIs de compras com as mesmas transformações da Silver/Gold."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, sha2, to_date, to_timestamp, trim

from rastro_publico.evidencias import metadados_evidencia
from rastro_publico.transformacoes.contratacoes import transformar_contratacoes
from rastro_publico.transformacoes.gold import (
    calcular_kpis_compras,
    preparar_relacoes_gold,
)
from rastro_publico.transformacoes.nucleo import (
    classificar_equipamentos,
    classificar_servicos,
    transformar_itens,
    transformar_resultados,
    transformar_vinculos_contratacao,
)


def _files(root: Path, pattern: str) -> list[str]:
    files = sorted(root.glob(pattern))
    if not files:
        raise FileNotFoundError(pattern)
    return [file.resolve().as_uri() for file in files]


def _read(spark: SparkSession, paths: list[str], dataset: str) -> DataFrame:
    parts = [
        spark.read.options(header=True, multiLine=True, escape='"')
        .csv(path)
        .withColumn("_source_file_id", sha2(lit(path), 256))
        .withColumn("_run_id", lit("auditoria-local"))
        .withColumn("_sistema_origem", lit("comprasgov"))
        .withColumn("_dataset_origem", lit(dataset))
        .withColumn("_coletado_em_utc", lit("2026-07-18T00:00:00Z"))
        for path in paths
    ]
    result = parts[0]
    for part in parts[1:]:
        result = result.unionByName(part, allowMissingColumns=True)
    return result


def preparar_silver_compras(
    compras_raw: DataFrame,
    itens_raw: DataFrame,
    resultados_raw: DataFrame,
    segredo: str = "auditoria-local-nao-producao",
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    contratacoes = transformar_contratacoes(compras_raw)[0]
    itens = classificar_servicos(
        classificar_equipamentos(transformar_itens(itens_raw)[0])
    ).join(contratacoes.select("contratacao_id"), "contratacao_id", "inner")
    resultados = transformar_resultados(resultados_raw, segredo)[0].join(
        itens.select("item_id"), "item_id", "inner"
    )
    vinculos = transformar_vinculos_contratacao(compras_raw).join(
        contratacoes.select("contratacao_id"), "contratacao_id", "inner"
    )
    return itens, resultados, contratacoes, vinculos


def auditar_compras_brutas(
    compras_raw: DataFrame,
    itens_raw: DataFrame,
    resultados_raw: DataFrame,
    segredo: str = "auditoria-local-nao-producao",
) -> DataFrame:
    return calcular_kpis_compras(
        *preparar_silver_compras(compras_raw, itens_raw, resultados_raw, segredo)
    )


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
    compras_raw = _read(
        spark, _files(args.root, "*VW_FT_PNCP_COMPRA-*.csv"), "VW_FT_PNCP_COMPRA"
    ).where(
        to_date(to_timestamp("data_publicacao_pncp")).between(
            args.data_inicio, args.data_fim
        )
    )
    chaves = compras_raw.select(
        trim("numero_controle_PNCP").alias("numero_controle_pncp")
    ).where("numero_controle_pncp IS NOT NULL").distinct()

    itens_raw = (
        _read(
            spark,
            _files(args.root, "*VW_FT_PNCP_COMPRA_ITEM-*.csv"),
            "VW_FT_PNCP_COMPRA_ITEM",
        )
        .withColumn(
            "numero_controle_pncp", trim("numero_controle_PNCP_compra")
        )
        .join(chaves, "numero_controle_pncp", "left_semi")
        .drop("numero_controle_pncp")
    )
    resultados_raw = (
        _read(
            spark,
            _files(args.root, "*VW_DM_PNCP_ITEM_RESULTADO-*.csv"),
            "VW_DM_PNCP_ITEM_RESULTADO",
        )
        .withColumn(
            "numero_controle_pncp", trim("numero_controle_PNCP_compra")
        )
        .join(chaves, "numero_controle_pncp", "left_semi")
        .drop("numero_controle_pncp")
    )

    itens, resultados, contratacoes, vinculos = preparar_silver_compras(
        compras_raw, itens_raw, resultados_raw
    )
    kpis = calcular_kpis_compras(
        itens, resultados, contratacoes, vinculos
    ).first()
    categorias = {
        row["categoria_tecnologia"]: row["count"]
        for row in preparar_relacoes_gold(
            itens, resultados, contratacoes, vinculos
        )
        .select("item_id", "categoria_tecnologia")
        .distinct()
        .groupBy("categoria_tecnologia")
        .count()
        .collect()
    }
    output = {
        "_meta": metadados_evidencia("audit_corrected_kpis", "VALIDADA"),
        "janela": {"inicio": args.data_inicio, "fim": args.data_fim},
        **kpis.asDict(),
        "itens_por_categoria": dict(sorted(categorias.items())),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
