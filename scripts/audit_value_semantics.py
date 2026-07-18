"""Audita a semântica de valor homologado usada pela Gold."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    lit,
    max as spark_max,
    percentile_approx,
    sha2,
    sum as spark_sum,
    to_date,
    to_timestamp,
    trim,
    when,
)

from rastro_publico.evidencias import metadados_evidencia
from rastro_publico.transformacoes.gold import avaliar_elegibilidade_monetaria
from rastro_publico.transformacoes.nucleo import (
    classificar_equipamentos,
    classificar_servicos,
    transformar_itens,
    transformar_resultados,
)


def _arquivos(padrao: str) -> list[str]:
    caminho = Path(padrao)
    caminhos = sorted(caminho.parent.glob(caminho.name))
    if not caminhos:
        raise FileNotFoundError(padrao)
    return [item.resolve().as_uri() for item in caminhos]


def _ler(spark: SparkSession, caminhos: list[str], dataset: str) -> DataFrame:
    partes = [
        spark.read.options(header=True, multiLine=True, escape='"')
        .csv(caminho)
        .withColumn("_source_file_id", sha2(lit(caminho), 256))
        .withColumn("_dataset_origem", lit(dataset))
        for caminho in caminhos
    ]
    resultado = partes[0]
    for parte in partes[1:]:
        resultado = resultado.unionByName(parte, allowMissingColumns=True)
    return resultado


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--compras", required=True)
    parser.add_argument("--itens", required=True)
    parser.add_argument("--resultados", required=True)
    parser.add_argument("--data-inicio", required=True)
    parser.add_argument("--data-fim", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.master("local[4]")
        .appName("rastro-publico-auditoria-valores-homologados")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    compras = _ler(spark, _arquivos(args.compras), "VW_FT_PNCP_COMPRA")
    chaves = (
        compras.where(
            to_date(to_timestamp("data_publicacao_pncp")).between(
                args.data_inicio, args.data_fim
            )
        )
        .select(trim("numero_controle_PNCP").alias("numero_controle_pncp"))
        .where("numero_controle_pncp IS NOT NULL")
        .distinct()
    )
    itens_raw = (
        _ler(spark, _arquivos(args.itens), "VW_FT_PNCP_COMPRA_ITEM")
        .withColumn(
            "numero_controle_pncp", trim("numero_controle_PNCP_compra")
        )
        .join(chaves, "numero_controle_pncp", "left_semi")
        .drop("numero_controle_pncp")
    )
    resultados_raw = (
        _ler(
            spark,
            _arquivos(args.resultados),
            "VW_DM_PNCP_ITEM_RESULTADO",
        )
        .withColumn(
            "numero_controle_pncp", trim("numero_controle_PNCP_compra")
        )
        .join(chaves, "numero_controle_pncp", "left_semi")
        .drop("numero_controle_pncp")
    )

    itens = classificar_servicos(classificar_equipamentos(transformar_itens(itens_raw)[0]))
    itens_tecnologia = itens.where("categoria_tecnologia <> 'incerto'").select(
        "item_id"
    )
    resultados = transformar_resultados(
        resultados_raw, "auditoria-local-nao-producao"
    )[0].join(itens_tecnologia, "item_id", "inner")
    avaliados = avaliar_elegibilidade_monetaria(resultados)

    resumo = avaliados.agg(
        count("*").alias("linhas_resultado"),
        countDistinct("resultado_id").alias("resultados_distintos"),
        spark_sum(when(col("cancelado"), 1).otherwise(0)).alias("cancelados"),
        spark_sum(
            when(col("valor_total_homologado").isNull(), 1).otherwise(0)
        ).alias("totais_nulos"),
        spark_sum(
            when(col("valor_total_homologado") < 0, 1).otherwise(0)
        ).alias("totais_negativos"),
        spark_sum(when(~col("elegivel_monetario"), 1).otherwise(0)).alias(
            "inelegiveis_monetarios"
        ),
        spark_sum(
            when(col("valor_total_homologado") > lit(1_000_000_000), 1).otherwise(0)
        ).alias("resultados_acima_1_bilhao"),
        spark_max("valor_total_homologado").alias("maximo_valor_total_homologado"),
        percentile_approx(
            "valor_total_homologado", [0.5, 0.9, 0.99, 0.999], 10000
        ).alias("percentis_valor_total_homologado"),
    ).first()
    motivos = {
        row["motivo_inelegibilidade_monetaria"]: row["count"]
        for row in avaliados.where("NOT elegivel_monetario")
        .groupBy("motivo_inelegibilidade_monetaria")
        .count()
        .collect()
    }
    maiores = [
        row.asDict(recursive=True)
        for row in avaliados.select(
            "resultado_id",
            "item_id",
            "quantidade_homologada",
            "valor_unitario_homologado",
            "valor_total_homologado",
            "cancelado",
            "elegivel_monetario",
            "motivo_inelegibilidade_monetaria",
        )
        .orderBy(col("valor_total_homologado").desc_nulls_last())
        .limit(20)
        .collect()
    ]
    saida = {
        "_meta": metadados_evidencia(
            "audit_value_semantics", "NAO_PUBLICAVEL"
        ),
        "janela": {"inicio": args.data_inicio, "fim": args.data_fim},
        "campo_auditado": "valor_total_homologado",
        "resumo_tecnologia": resumo.asDict(recursive=True),
        "motivos_inelegibilidade": dict(sorted(motivos.items())),
        "maiores_resultados": maiores,
        "publicabilidade_monetaria": "nao_publicavel_ate_validacao_da_fonte",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(saida, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    print(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
