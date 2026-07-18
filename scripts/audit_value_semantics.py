"""Audita a semântica dos valores de itens em uma janela de compras."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    count,
    lit,
    max as spark_max,
    percentile_approx,
    sum as spark_sum,
    to_date,
    to_timestamp,
    trim,
    when,
)

from rastro_publico.transformacoes.nucleo import (
    classificar_equipamentos,
    classificar_servicos,
)


def _arquivos(padrao: str) -> list[str]:
    caminhos = sorted(Path().glob(padrao)) if not Path(padrao).is_absolute() else []
    if not caminhos:
        caminho = Path(padrao)
        caminhos = sorted(caminho.parent.glob(caminho.name))
    if not caminhos:
        raise FileNotFoundError(padrao)
    return [caminho.resolve().as_uri() for caminho in caminhos]


def _ler_por_arquivo(spark: SparkSession, caminhos: list[str]):
    """Replica a união nominal usada na preparação anual."""
    partes = [
        spark.read.options(header=True, multiLine=True, escape='"').csv(caminho)
        for caminho in caminhos
    ]
    resultado = partes[0]
    for parte in partes[1:]:
        resultado = resultado.unionByName(parte, allowMissingColumns=True)
    return resultado


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--compras", required=True, help="glob dos CSVs de compras")
    parser.add_argument("--itens", required=True, help="glob dos CSVs de itens")
    parser.add_argument("--data-inicio", required=True)
    parser.add_argument("--data-fim", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.master("local[4]")
        .appName("rastro-publico-auditoria-valores")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    compras = _ler_por_arquivo(spark, _arquivos(args.compras))
    itens = _ler_por_arquivo(spark, _arquivos(args.itens))

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
    base = (
        itens.withColumn(
            "numero_controle_pncp", trim("numero_controle_PNCP_compra")
        )
        .join(chaves, "numero_controle_pncp", "left_semi")
        .select(
            "id_compra_item",
            "descricao_resumida",
            "material_ou_servico",
            "unidade_medida",
            col("quantidade").cast("decimal(38,6)").alias("quantidade"),
            col("valor_unitario_estimado")
            .cast("decimal(38,4)")
            .alias("valor_unitario"),
            col("valor_total").cast("decimal(38,2)").alias("valor_total"),
        )
        .withColumn("valor_calculado", col("quantidade") * col("valor_unitario"))
        .withColumn(
            "erro_relativo",
            when(
                col("valor_total") > 0,
                spark_abs(col("valor_total") - col("valor_calculado"))
                / col("valor_total"),
            ),
        )
    )

    base = classificar_servicos(
        classificar_equipamentos(
            base.withColumn("descricao", col("descricao_resumida")).withColumn(
                "material_ou_servico", col("material_ou_servico")
            )
        )
    )

    def resumir(populacao):
        return populacao.agg(
            count("*").alias("itens"),
            spark_sum("valor_total").alias("soma_valor_total"),
            spark_max("valor_total").alias("maximo_valor_total"),
            percentile_approx(
                "valor_total", [0.5, 0.9, 0.99, 0.999], 10000
            ).alias("percentis_valor_total"),
            spark_sum(
                when(col("valor_total") > lit(1_000_000_000), 1).otherwise(0)
            ).alias("itens_acima_1_bilhao"),
            spark_sum(
                when(col("valor_total") > lit(1_000_000_000_000), 1).otherwise(0)
            ).alias("itens_acima_1_trilhao"),
            spark_sum(
                when(
                    col("valor_total") <= lit(1_000_000_000), col("valor_total")
                ).otherwise(0)
            ).alias("soma_ate_1_bilhao"),
            spark_sum(
                when(col("erro_relativo") > lit(0.01), 1).otherwise(0)
            ).alias("divergencias_quantidade_vezes_unitario"),
        ).first()

    resumo = resumir(base)
    tecnologia = base.where("categoria_tecnologia <> 'incerto'")
    resumo_tecnologia = resumir(tecnologia)
    maiores = [
        row.asDict(recursive=True)
        for row in base.orderBy(col("valor_total").desc_nulls_last()).limit(20).collect()
    ]
    saida = {
        "janela": {"inicio": args.data_inicio, "fim": args.data_fim},
        "resumo": resumo.asDict(recursive=True),
        "resumo_tecnologia": resumo_tecnologia.asDict(recursive=True),
        "maiores_itens": maiores,
    }
    assert saida["resumo"]["itens"] > 0
    assert all(
        maiores[indice]["valor_total"] >= maiores[indice + 1]["valor_total"]
        for indice in range(len(maiores) - 1)
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(saida, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
