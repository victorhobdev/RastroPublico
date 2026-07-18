from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    coalesce,
    count,
    countDistinct,
    datediff,
    date_format,
    first,
    greatest,
    lit,
    max as spark_max,
    min as spark_min,
    percentile_approx,
    row_number,
    sum as spark_sum,
    to_date,
    to_timestamp,
    when,
)


def calcular_qualidade_cobertura(
    itens: DataFrame,
    resultados: DataFrame,
    contratacoes: DataFrame,
    vinculos: DataFrame,
    qualidade_operacional: DataFrame,
) -> DataFrame:
    por_item = resultados.groupBy("item_id").agg(
        countDistinct("resultado_id").alias("resultados"),
        countDistinct(
            when(col("fornecedor_id").isNotNull(), col("resultado_id"))
        ).alias("resultados_com_fornecedor"),
    )
    base = (
        itens.join(
            contratacoes.select(
                "contratacao_id", "publicado_em", "modalidade_id", "modalidade"
            ),
            "contratacao_id",
            "left",
        )
        .join(vinculos.select("contratacao_id", "orgao_id"), "contratacao_id", "left")
        .join(por_item, "item_id", "left")
        .withColumn(
            "periodo",
            coalesce(
                date_format(to_timestamp("publicado_em"), "yyyy-MM"),
                lit("nao_informado"),
            ),
        )
        .withColumn(
            "modalidade_id", coalesce(col("modalidade_id"), lit("nao_informada"))
        )
        .withColumn("modalidade", coalesce(col("modalidade"), lit("Nao informada")))
        .withColumn(
            "categoria_tecnologia",
            coalesce(col("categoria_tecnologia"), lit("incerto")),
        )
    )
    cobertura = (
        base.groupBy("periodo", "modalidade_id", "modalidade", "categoria_tecnologia")
        .agg(
            countDistinct("item_id").alias("total_itens"),
            countDistinct(when(col("resultados") > 0, col("item_id"))).alias(
                "itens_com_resultado"
            ),
            spark_sum(coalesce(col("resultados"), lit(0))).alias("resultados"),
            spark_sum(coalesce(col("resultados_com_fornecedor"), lit(0))).alias(
                "resultados_com_fornecedor"
            ),
            countDistinct(
                when(col("unidade_medida").isNotNull(), col("item_id"))
            ).alias("itens_com_unidade"),
            countDistinct(when(col("quantidade") > 0, col("item_id"))).alias(
                "itens_com_quantidade"
            ),
            countDistinct(
                when(col("valor_unitario_estimado").isNotNull(), col("item_id"))
            ).alias("itens_com_preco_unitario"),
            countDistinct(
                when(col("categoria_tecnologia") != "incerto", col("item_id"))
            ).alias("itens_com_categoria"),
            countDistinct(when(col("orgao_id").isNotNull(), col("item_id"))).alias(
                "itens_com_orgao"
            ),
        )
        .withColumn(
            "cobertura_resultado", col("itens_com_resultado") / col("total_itens")
        )
        .withColumn(
            "cobertura_fornecedor",
            when(
                col("resultados") > 0,
                col("resultados_com_fornecedor") / col("resultados"),
            ).otherwise(lit(0.0)),
        )
        .withColumn("cobertura_unidade", col("itens_com_unidade") / col("total_itens"))
        .withColumn(
            "cobertura_quantidade", col("itens_com_quantidade") / col("total_itens")
        )
        .withColumn(
            "cobertura_preco_unitario",
            col("itens_com_preco_unitario") / col("total_itens"),
        )
        .withColumn(
            "cobertura_categoria", col("itens_com_categoria") / col("total_itens")
        )
        .withColumn("cobertura_orgao", col("itens_com_orgao") / col("total_itens"))
        .withColumn("entidade", lit("item_contratacao"))
        .withColumn("metrica", lit("cobertura_analitica"))
        .withColumn("observados", lit(None).cast("long"))
        .withColumn("total", lit(None).cast("long"))
        .withColumn("percentual", lit(None).cast("double"))
        .withColumn("severidade", lit(None).cast("string"))
        .withColumn("status_qualidade", lit("PASS"))
        .withColumn("status_publicacao", lit("publicada"))
    )

    ordem = Window.partitionBy("regra").orderBy(
        to_timestamp("registrado_em").desc(), col("run_id").desc()
    )
    operacional = (
        qualidade_operacional.withColumn("_ordem", row_number().over(ordem))
        .where("_ordem = 1")
        .select(
            lit("janela_atual").alias("periodo"),
            lit("todos").alias("modalidade_id"),
            lit("Todas").alias("modalidade"),
            lit("todas").alias("categoria_tecnologia"),
            lit("pipeline").alias("entidade"),
            col("regra").alias("metrica"),
            col("observados").cast("long"),
            col("total").cast("long"),
            col("percentual").cast("double"),
            "severidade",
            col("status").alias("status_qualidade"),
        )
        .withColumn("status_publicacao", lit("publicada"))
    )
    for nome in (
        "total_itens",
        "itens_com_resultado",
        "resultados",
        "resultados_com_fornecedor",
        "itens_com_unidade",
        "itens_com_quantidade",
        "itens_com_preco_unitario",
        "itens_com_categoria",
        "itens_com_orgao",
    ):
        operacional = operacional.withColumn(nome, lit(None).cast("long"))
    for nome in (
        "cobertura_resultado",
        "cobertura_fornecedor",
        "cobertura_unidade",
        "cobertura_quantidade",
        "cobertura_preco_unitario",
        "cobertura_categoria",
        "cobertura_orgao",
    ):
        operacional = operacional.withColumn(nome, lit(None).cast("double"))
    return cobertura.unionByName(operacional, allowMissingColumns=True)


def calcular_concentracao_fornecedores(
    itens: DataFrame,
    resultados: DataFrame,
    contratacoes: DataFrame,
    vinculos: DataFrame,
    *,
    minimo_fornecedores: int = 2,
    minimo_resultados: int = 3,
    cobertura_minima: float = 0.8,
) -> DataFrame:
    dimensoes = [
        "periodo",
        "orgao_id",
        "categoria_tecnologia",
        "modalidade_id",
        "modalidade",
    ]
    base = (
        resultados.select(
            "resultado_id",
            "item_id",
            "fornecedor_id",
            "valor_total_homologado",
            "cancelado",
        ).join(
            itens.select("item_id", "contratacao_id", "categoria_tecnologia"),
            "item_id",
        )
        .join(
            contratacoes.select(
                "contratacao_id", "publicado_em", "modalidade_id", "modalidade"
            ),
            "contratacao_id",
        )
        .join(vinculos.select("contratacao_id", "orgao_id"), "contratacao_id")
        .withColumn("periodo", date_format(to_timestamp("publicado_em"), "yyyy-MM"))
        .where(
            (col("categoria_tecnologia") != "incerto")
            & col("periodo").isNotNull()
            & col("orgao_id").isNotNull()
            & col("modalidade_id").isNotNull()
            & (~col("cancelado"))
        )
    )
    denominadores = base.groupBy(*dimensoes).agg(
        countDistinct("resultado_id").alias("resultados_observados"),
        spark_sum(
            when(
                col("valor_total_homologado") > 0, col("valor_total_homologado")
            ).otherwise(0)
        ).alias("valor_observado"),
    )
    elegiveis = base.where(
        col("fornecedor_id").isNotNull() & (col("valor_total_homologado") > 0)
    )
    por_fornecedor = elegiveis.groupBy(*dimensoes, "fornecedor_id").agg(
        spark_sum("valor_total_homologado").alias("valor_fornecedor"),
        countDistinct("resultado_id").alias("resultados_fornecedor"),
    )
    janela = Window.partitionBy(*dimensoes)
    ranking = Window.partitionBy(*dimensoes).orderBy(
        col("valor_fornecedor").desc(), col("fornecedor_id")
    )
    participacoes = (
        por_fornecedor.withColumn(
            "valor_total_homologado", spark_sum("valor_fornecedor").over(janela)
        )
        .withColumn(
            "participacao", col("valor_fornecedor") / col("valor_total_homologado")
        )
        .withColumn("posicao", row_number().over(ranking))
    )
    concentracao = participacoes.groupBy(*dimensoes).agg(
        first("valor_total_homologado").alias("valor_total_homologado"),
        count("fornecedor_id").alias("fornecedores_distintos"),
        spark_sum("resultados_fornecedor").alias("resultados_elegiveis"),
        spark_max("participacao").alias("top_1"),
        spark_sum(when(col("posicao") <= 3, col("participacao")).otherwise(0)).alias(
            "top_3"
        ),
        spark_sum(col("participacao") * col("participacao")).alias("hhi"),
    )
    return (
        concentracao.join(denominadores, dimensoes)
        .withColumn(
            "cobertura_valor",
            when(
                col("valor_observado") > 0,
                col("valor_total_homologado") / col("valor_observado"),
            ).otherwise(lit(0.0)),
        )
        .withColumn(
            "limitacao",
            when(
                col("resultados_elegiveis") < minimo_resultados,
                "resultados_insuficientes",
            )
            .when(
                col("fornecedores_distintos") < minimo_fornecedores,
                "fornecedores_insuficientes",
            )
            .when(
                col("cobertura_valor") < cobertura_minima,
                "cobertura_valor_insuficiente",
            ),
        )
        .withColumn(
            "status_publicacao",
            when(col("limitacao").isNull(), "publicada").otherwise("nao_publicavel"),
        )
    )


def calcular_cobertura_servicos(itens: DataFrame) -> DataFrame:
    return (
        itens.where(
            col("categoria_servico").isNotNull()
            & (col("categoria_servico") != "incerto")
        )
        .groupBy("categoria_servico")
        .agg(
            countDistinct("item_id").alias("total_itens"),
            countDistinct(
                when(col("unidade_medida").isNotNull(), col("item_id"))
            ).alias("itens_com_unidade"),
            countDistinct(
                when(col("valor_unitario_estimado").isNotNull(), col("item_id"))
            ).alias("itens_com_preco"),
            countDistinct("unidade_medida").alias("unidades_distintas"),
        )
        .withColumn("cobertura_unidade", col("itens_com_unidade") / col("total_itens"))
        .withColumn("cobertura_preco", col("itens_com_preco") / col("total_itens"))
        .withColumn("status_publicacao_preco", lit("nao_publicavel"))
        .withColumn("limitacao_preco", lit("escopo_unidade_sla_nao_estruturados"))
    )


def _base_relacoes(
    itens: DataFrame,
    resultados: DataFrame,
    contratacoes: DataFrame,
    vinculos: DataFrame,
) -> DataFrame:
    return (
        resultados.select(
            "resultado_id",
            "item_id",
            "fornecedor_id",
            "valor_total_homologado",
            "cancelado",
        ).join(
            itens.select("item_id", "contratacao_id", "categoria_tecnologia"),
            "item_id",
        )
        .join(
            contratacoes.select(
                "contratacao_id", "publicado_em", "modalidade_id", "modalidade"
            ),
            "contratacao_id",
        )
        .join(
            vinculos.select("contratacao_id", "orgao_id", "unidade_id"),
            "contratacao_id",
        )
        .withColumn("data_publicacao", to_date(to_timestamp("publicado_em")))
        .withColumn("periodo", date_format("data_publicacao", "yyyy-MM"))
        .where(
            (~col("cancelado"))
            & col("fornecedor_id").isNotNull()
            & col("orgao_id").isNotNull()
            & col("data_publicacao").isNotNull()
            & col("categoria_tecnologia").isNotNull()
            & (col("categoria_tecnologia") != "incerto")
        )
    )


def calcular_recorrencia_orgao_fornecedor(
    itens: DataFrame,
    resultados: DataFrame,
    contratacoes: DataFrame,
    vinculos: DataFrame,
) -> DataFrame:
    return (
        _base_relacoes(itens, resultados, contratacoes, vinculos)
        .groupBy("orgao_id", "fornecedor_id", "categoria_tecnologia")
        .agg(
            countDistinct("contratacao_id").alias("contratacoes_distintas"),
            countDistinct("resultado_id").alias("resultados_distintos"),
            countDistinct("periodo").alias("periodos_distintos"),
            spark_min("data_publicacao").alias("primeira_ocorrencia"),
            spark_max("data_publicacao").alias("ultima_ocorrencia"),
            spark_sum(
                when(col("valor_total_homologado") > 0, col("valor_total_homologado"))
                .otherwise(0)
                .cast("decimal(38,2)")
            ).alias("valor_total_homologado"),
        )
        .withColumn(
            "dias_entre_primeira_ultima",
            datediff("ultima_ocorrencia", "primeira_ocorrencia"),
        )
        .withColumn("status_publicacao", lit("publicada"))
        .withColumn(
            "limitacao",
            lit("recorrencia_descritiva_nao_indica_irregularidade"),
        )
    )


def calcular_presenca_fornecedores(
    itens: DataFrame,
    resultados: DataFrame,
    contratacoes: DataFrame,
    vinculos: DataFrame,
    unidades: DataFrame,
) -> DataFrame:
    return (
        _base_relacoes(itens, resultados, contratacoes, vinculos)
        .join(unidades.select("unidade_id", "uf", "municipio"), "unidade_id", "left")
        .groupBy("fornecedor_id", "categoria_tecnologia")
        .agg(
            countDistinct("orgao_id").alias("orgaos_distintos"),
            countDistinct("unidade_id").alias("unidades_distintas"),
            countDistinct("uf").alias("ufs_distintas"),
            countDistinct("municipio").alias("municipios_distintos"),
            countDistinct("modalidade_id").alias("modalidades_distintas"),
            countDistinct("periodo").alias("periodos_distintos"),
            countDistinct("contratacao_id").alias("contratacoes_distintas"),
            spark_sum(
                when(col("valor_total_homologado") > 0, col("valor_total_homologado"))
                .otherwise(0)
                .cast("decimal(38,2)")
            ).alias("valor_total_homologado"),
        )
        .withColumn("status_publicacao", lit("publicada"))
        .withColumn("limitacao", lit("presenca_restrita_a_resultados_homologados"))
    )


def calcular_rede_orgao_fornecedor(
    itens: DataFrame,
    resultados: DataFrame,
    contratacoes: DataFrame,
    vinculos: DataFrame,
) -> DataFrame:
    return (
        _base_relacoes(itens, resultados, contratacoes, vinculos)
        .groupBy("orgao_id", "fornecedor_id", "categoria_tecnologia")
        .agg(
            countDistinct("contratacao_id").alias("contratacoes_distintas"),
            countDistinct("resultado_id").alias("resultados_distintos"),
            countDistinct("periodo").alias("periodos_distintos"),
            countDistinct("modalidade_id").alias("modalidades_distintas"),
            spark_sum(
                when(col("valor_total_homologado") > 0, col("valor_total_homologado"))
                .otherwise(0)
                .cast("decimal(38,2)")
            ).alias("valor_total_homologado"),
        )
        .withColumn("status_publicacao", lit("publicada"))
        .withColumn(
            "limitacao", lit("aresta_descritiva_nao_indica_irregularidade")
        )
    )


def calcular_variacao_precos(
    itens: DataFrame, resultados: DataFrame, contratacoes: DataFrame
) -> DataFrame:
    return (
        resultados.select(
            "resultado_id",
            "item_id",
            "valor_unitario_homologado",
            "cancelado",
        ).join(
            itens.select(
                "item_id",
                "contratacao_id",
                "categoria_tecnologia",
                "unidade_medida",
                "material_ou_servico",
            ),
            "item_id",
        )
        .join(contratacoes.select("contratacao_id", "publicado_em"), "contratacao_id")
        .withColumn("periodo", date_format(to_timestamp("publicado_em"), "yyyy-MM"))
        .where(
            (~col("cancelado"))
            & (col("material_ou_servico") == "M")
            & (col("categoria_tecnologia") != "incerto")
            & col("unidade_medida").isNotNull()
            & (col("valor_unitario_homologado") > 0)
            & col("periodo").isNotNull()
        )
        .groupBy("periodo", "categoria_tecnologia", "unidade_medida")
        .agg(
            countDistinct("resultado_id").alias("observacoes"),
            percentile_approx("valor_unitario_homologado", 0.1, 10000).alias("p10"),
            percentile_approx("valor_unitario_homologado", 0.5, 10000).alias(
                "mediana"
            ),
            percentile_approx("valor_unitario_homologado", 0.9, 10000).alias("p90"),
            spark_min("valor_unitario_homologado").alias("minimo"),
            spark_max("valor_unitario_homologado").alias("maximo"),
        )
        .withColumn("status_publicacao", lit("nao_publicavel"))
        .withColumn(
            "limitacao", lit("produto_nao_homogeneo_em_categoria_unidade")
        )
    )


def calcular_evolucao_contratual(
    contratos: DataFrame, eventos: DataFrame
) -> DataFrame:
    resumo_eventos = eventos.groupBy("contrato_id").agg(
        countDistinct("evento_contrato_id").alias("eventos_distintos"),
        countDistinct("tipo_evento").alias("tipos_evento_distintos"),
        spark_min(to_date("data_assinatura")).alias("primeiro_evento_em"),
        spark_max(to_date("data_assinatura")).alias("ultimo_evento_em"),
        spark_max(to_date("vigencia_fim")).alias("vigencia_fim_eventos"),
        spark_sum(coalesce(col("variacao_valor"), lit(0))).alias(
            "variacao_valor_acumulada"
        ),
    )
    return (
        contratos.select(
            "contrato_id",
            "orgao_codigo",
            "fornecedor_id",
            to_date("vigencia_inicio").alias("vigencia_inicio"),
            to_date("vigencia_fim").alias("vigencia_fim_original"),
            "valor_inicial",
            "valor_global",
        )
        .join(resumo_eventos, "contrato_id", "left")
        .fillna(
            {
                "eventos_distintos": 0,
                "tipos_evento_distintos": 0,
                "variacao_valor_acumulada": 0,
            }
        )
        .withColumn(
            "vigencia_fim_atual",
            greatest("vigencia_fim_original", "vigencia_fim_eventos"),
        )
        .withColumn(
            "extensao_vigencia_dias",
            when(
                col("vigencia_fim_original").isNotNull(),
                greatest(
                    datediff("vigencia_fim_atual", "vigencia_fim_original"), lit(0)
                ),
            ),
        )
        .withColumn("status_publicacao", lit("publicada"))
        .withColumn(
            "limitacao",
            lit("fonte_comprasnet_com_vinculo_pncp_nativo_parcial_c3"),
        )
    )
