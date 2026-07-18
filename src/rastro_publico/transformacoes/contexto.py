from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    lit,
    max as spark_max,
    regexp_replace,
    substring,
    sum as spark_sum,
    to_date,
    trim,
    when,
)


def transformar_empresas_cnpj(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        regexp_replace("cnpj_basico", r"\D", "").alias("cnpj_basico"),
        trim("razao_social").alias("razao_social_receita"),
        trim("natureza_juridica").alias("natureza_juridica"),
        regexp_replace("capital_social", ",", ".")
        .cast("decimal(38,2)")
        .alias("capital_social"),
        trim("porte_empresa").alias("porte_empresa"),
    ).where("length(cnpj_basico) = 8")


def resumir_qsa(bronze: DataFrame) -> DataFrame:
    return (
        bronze.select(
            regexp_replace("cnpj_basico", r"\D", "").alias("cnpj_basico"),
            trim("tipo_socio").alias("tipo_socio"),
            to_date("data_entrada_sociedade", "yyyyMMdd").alias(
                "data_entrada_sociedade"
            ),
        )
        .where("length(cnpj_basico) = 8")
        .groupBy("cnpj_basico")
        .agg(
            count(lit(1)).alias("registros_socios"),
            spark_sum(when(col("tipo_socio") == "1", 1).otherwise(0)).alias(
                "socios_pessoa_juridica"
            ),
            spark_sum(when(col("tipo_socio") == "2", 1).otherwise(0)).alias(
                "socios_pessoa_fisica"
            ),
            spark_sum(when(col("tipo_socio") == "3", 1).otherwise(0)).alias(
                "socios_estrangeiros"
            ),
            spark_max("data_entrada_sociedade").alias("entrada_socio_mais_recente"),
        )
    )


def transformar_sancoes(bronze: DataFrame) -> DataFrame:
    return (
        bronze.select(
            regexp_replace("cnpj_sancionado", r"\D", "").alias("cnpj_sancionado"),
            trim("cadastro").alias("cadastro"),
            trim("codigo_sancao").alias("codigo_sancao"),
            to_date("data_inicio_sancao", "dd/MM/yyyy").alias("data_inicio_sancao"),
            to_date("data_final_sancao", "dd/MM/yyyy").alias("data_final_sancao"),
        )
        .where("length(cnpj_sancionado) = 14")
        .groupBy("cnpj_sancionado")
        .agg(
            countDistinct("codigo_sancao").alias("sancoes_distintas"),
            countDistinct(when(col("cadastro") == "CEIS", col("codigo_sancao"))).alias(
                "sancoes_ceis"
            ),
            countDistinct(when(col("cadastro") == "CNEP", col("codigo_sancao"))).alias(
                "sancoes_cnep"
            ),
            spark_max("data_inicio_sancao").alias("sancao_inicio_mais_recente"),
            spark_max("data_final_sancao").alias("sancao_final_mais_recente"),
        )
    )


def enriquecer_fornecedores_contexto(
    fornecedores: DataFrame,
    empresas: DataFrame,
    qsa: DataFrame,
    sancoes: DataFrame,
) -> DataFrame:
    return (
        fornecedores.where("length(regexp_replace(identificador_publico, '\\D', '')) = 14")
        .select(
            "fornecedor_id",
            regexp_replace("identificador_publico", r"\D", "").alias("cnpj"),
            "nome_fornecedor",
        )
        .dropDuplicates(["fornecedor_id"])
        .withColumn("cnpj_basico", substring("cnpj", 1, 8))
        .join(empresas, "cnpj_basico", "left")
        .join(qsa, "cnpj_basico", "left")
        .join(sancoes, col("cnpj") == col("cnpj_sancionado"), "left")
        .drop("cnpj_sancionado")
        .fillna(
            {
                "registros_socios": 0,
                "socios_pessoa_juridica": 0,
                "socios_pessoa_fisica": 0,
                "socios_estrangeiros": 0,
                "sancoes_distintas": 0,
                "sancoes_ceis": 0,
                "sancoes_cnep": 0,
            }
        )
        .withColumn(
            "cobertura_cnpj_receita",
            when(col("razao_social_receita").isNotNull(), lit(1.0)).otherwise(lit(0.0)),
        )
        .withColumn("status_publicacao", lit("publicada"))
        .withColumn(
            "interpretacao", lit("contexto_cadastral_nao_indica_irregularidade")
        )
    )
