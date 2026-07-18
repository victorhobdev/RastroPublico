from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    concat,
    countDistinct,
    lit,
    lower,
    row_number,
    sha2,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import DecimalType

from rastro_publico.transformacoes.nucleo import adicionar_hash_conteudo


def transformar_contratacoes(
    bronze: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    tipadas = (
        bronze.select(
            trim(col("id_compra")).alias("id_origem"),
            trim(col("numero_controle_PNCP")).alias("numero_controle_pncp"),
            trim(col("orgao_entidade_cnpj")).alias("cnpj_orgao"),
            trim(col("objeto_compra")).alias("objeto"),
            col("valor_total_estimado")
            .cast(DecimalType(38, 2))
            .alias("valor_total_estimado"),
            to_timestamp("data_publicacao_pncp").alias("publicado_em"),
            trim(col("modalidade_id_pncp")).alias("modalidade_id"),
            trim(col("modalidade_nome")).alias("modalidade"),
            to_timestamp("data_atualizacao_pncp").alias("atualizado_em"),
            col("ind_atual").cast("boolean").alias("ind_atual"),
            col("contratacao_excluida").cast("boolean").alias("contratacao_excluida"),
            col("_sistema_origem").alias("sistema_origem"),
            col("_source_file_id").alias("source_file_id"),
            col("_run_id").alias("run_id"),
            to_timestamp("_coletado_em_utc").alias("coletado_em_utc"),
        )
        .withColumn(
            "chave_natural",
            when(
                col("numero_controle_pncp").isNotNull()
                & (col("numero_controle_pncp") != ""),
                concat(lit("pncp|"), lower(col("numero_controle_pncp"))),
            ).otherwise(
                concat(lower(col("sistema_origem")), lit("|"), col("id_origem"))
            ),
        )
        .withColumn("contratacao_id", sha2("chave_natural", 256))
        .withColumn(
            "motivo_quarentena",
            when(
                col("cnpj_orgao").isNull() | (col("cnpj_orgao") == ""),
                "cnpj_orgao_ausente",
            )
            .when(
                col("id_origem").isNull() | (col("id_origem") == ""),
                "id_origem_ausente",
            )
            .when(col("atualizado_em").isNull(), "data_atualizacao_invalida")
            .when(col("valor_total_estimado") < 0, "valor_total_negativo"),
        )
    )

    quarentena = tipadas.where(col("motivo_quarentena").isNotNull())
    validas = tipadas.where(col("motivo_quarentena").isNull()).drop("motivo_quarentena")
    campos_conteudo = [
        "numero_controle_pncp",
        "cnpj_orgao",
        "objeto",
        "valor_total_estimado",
        "publicado_em",
        "modalidade_id",
        "modalidade",
        "atualizado_em",
        "ind_atual",
        "contratacao_excluida",
    ]
    validas = adicionar_hash_conteudo(
        validas, campos_conteudo, "contratacao_v1"
    )
    chaves_conflito = (
        validas.groupBy("contratacao_id", "atualizado_em")
        .agg(countDistinct("hash_conteudo_entidade").alias("total_hashes"))
        .where("total_hashes > 1")
        .drop("total_hashes")
    )
    conflitos = validas.join(
        chaves_conflito, ["contratacao_id", "atualizado_em"], "inner"
    )
    elegiveis = validas.join(
        chaves_conflito, ["contratacao_id", "atualizado_em"], "left_anti"
    )
    janela = Window.partitionBy("contratacao_id").orderBy(
        col("atualizado_em").desc(),
        col("coletado_em_utc").desc(),
        col("source_file_id").desc(),
    )
    correntes = (
        elegiveis.withColumn("_ordem", row_number().over(janela))
        .where("_ordem = 1")
        .drop("_ordem")
    )
    return correntes, quarentena, conflitos
