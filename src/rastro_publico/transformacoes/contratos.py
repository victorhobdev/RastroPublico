from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    concat_ws,
    lit,
    lower,
    regexp_extract,
    regexp_replace,
    sha2,
    to_date,
    trim,
    try_to_timestamp,
    udf,
    when,
)
from pyspark.sql.types import DecimalType, StringType

from rastro_publico.transformacoes.nucleo import (
    _coluna_ou_nulo,
    _separar_versoes,
    classificar_equipamentos,
    classificar_servicos,
    pseudonimizar_identificador,
)


def filtrar_populacao_contratual_tecnologia(
    contratos: DataFrame,
    itens: DataFrame,
    eventos: DataFrame,
    fornecedores: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    contratos_tecnologia = (
        itens.where(
            col("categoria_tecnologia").isNotNull()
            & (col("categoria_tecnologia") != "incerto")
        )
        .select("contrato_id")
        .distinct()
    )
    contratos_filtrados = contratos.join(
        contratos_tecnologia, "contrato_id", "left_semi"
    )
    eventos_filtrados = eventos.join(
        contratos_tecnologia, "contrato_id", "left_semi"
    )
    fornecedores_filtrados = fornecedores.join(
        contratos_filtrados.select("fornecedor_id").distinct(),
        "fornecedor_id",
        "left_semi",
    )
    return contratos_filtrados, eventos_filtrados, fornecedores_filtrados


def transformar_contratos(
    bronze: DataFrame, segredo: str
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    pseudonimizar = udf(
        lambda valor: pseudonimizar_identificador(segredo, valor) if valor else None,
        StringType(),
    )
    identificador = regexp_replace(
        _coluna_ou_nulo(bronze, "fonecedor_cnpj_cpf_idgener"), r"\D", ""
    )
    tipo_pessoa_origem = lower(_coluna_ou_nulo(bronze, "fornecedor_tipo"))
    tipo_pessoa = (
        when(tipo_pessoa_origem.contains("fisica"), "PF")
        .when(tipo_pessoa_origem.contains("juridica"), "PJ")
        .otherwise("DESCONHECIDO")
    )
    tipadas = (
        bronze.select(
            trim(_coluna_ou_nulo(bronze, "id")).alias("id_origem_contrato"),
            trim(_coluna_ou_nulo(bronze, "numero")).alias("numero_contrato"),
            trim(_coluna_ou_nulo(bronze, "orgao_codigo")).alias("orgao_codigo"),
            trim(_coluna_ou_nulo(bronze, "orgao_nome")).alias("orgao_nome"),
            trim(_coluna_ou_nulo(bronze, "unidade_codigo")).alias("unidade_codigo"),
            trim(_coluna_ou_nulo(bronze, "unidade_nome")).alias("unidade_nome"),
            tipo_pessoa.alias("tipo_pessoa"),
            identificador.alias("identificador_normalizado"),
            trim(_coluna_ou_nulo(bronze, "fornecedor_nome")).alias(
                "nome_fornecedor"
            ),
            trim(_coluna_ou_nulo(bronze, "processo")).alias("processo"),
            trim(_coluna_ou_nulo(bronze, "objeto")).alias("objeto"),
            trim(_coluna_ou_nulo(bronze, "tipo")).alias("tipo_contrato"),
            trim(_coluna_ou_nulo(bronze, "categoria")).alias("categoria_contrato"),
            trim(_coluna_ou_nulo(bronze, "modalidade")).alias("modalidade"),
            _date(bronze, "data_assinatura").alias(
                "data_assinatura"
            ),
            _date(bronze, "data_publicacao").alias(
                "data_publicacao"
            ),
            _date(bronze, "vigencia_inicio").alias(
                "vigencia_inicio"
            ),
            _date(bronze, "vigencia_fim").alias("vigencia_fim"),
            _decimal(bronze, "valor_inicial", 2).alias("valor_inicial"),
            _decimal(bronze, "valor_global", 2).alias("valor_global"),
            _decimal(bronze, "valor_acumulado", 2).alias("valor_acumulado"),
            trim(_coluna_ou_nulo(bronze, "situacao")).alias("situacao_contrato"),
            coalesce(
                try_to_timestamp(
                    _coluna_ou_nulo(bronze, "_data_publicacao_arquivo")
                ),
                try_to_timestamp(_coluna_ou_nulo(bronze, "_coletado_em_utc")),
            ).alias("atualizado_em"),
            col("_source_file_id").alias("source_file_id"),
        )
        .withColumn(
            "contrato_id",
            sha2(concat(lit("comprasnet_contratos|"), col("id_origem_contrato")), 256),
        )
        .withColumn(
            "chave_fornecedor",
            concat_ws("|", "tipo_pessoa", lit("BRA"), "identificador_normalizado"),
        )
        .withColumn(
            "fornecedor_id",
            when(
                col("tipo_pessoa") == "PF", pseudonimizar("chave_fornecedor")
            )
            .when(col("tipo_pessoa") == "PJ", sha2("chave_fornecedor", 256))
            .otherwise(pseudonimizar("chave_fornecedor")),
        )
        .withColumn(
            "identificador_publico",
            when(col("tipo_pessoa") == "PJ", col("identificador_normalizado")),
        )
        .withColumn(
            "motivo_quarentena",
            when(
                col("id_origem_contrato").isNull()
                | (col("id_origem_contrato") == ""),
                "id_contrato_ausente",
            )
            .when(
                col("identificador_normalizado").isNull()
                | (col("identificador_normalizado") == ""),
                "fornecedor_ausente",
            )
            .when(col("valor_global") < 0, "valor_global_negativo")
            .when(col("valor_inicial") < 0, "valor_inicial_negativo")
            .when(
                col("vigencia_inicio").isNotNull()
                & col("vigencia_fim").isNotNull()
                & (col("vigencia_fim") < col("vigencia_inicio")),
                "vigencia_incoerente",
            )
            .when(col("atualizado_em").isNull(), "observacao_invalida"),
        )
    )
    quarentena = tipadas.where(col("motivo_quarentena").isNotNull()).drop(
        "identificador_normalizado", "chave_fornecedor"
    )
    validas = tipadas.where(col("motivo_quarentena").isNull()).drop(
        "motivo_quarentena"
    )
    correntes, conflitos = _separar_versoes(
        validas,
        ["contrato_id", "atualizado_em"],
        [
            "numero_contrato",
            "orgao_codigo",
            "unidade_codigo",
            "fornecedor_id",
            "processo",
            "objeto",
            "data_assinatura",
            "data_publicacao",
            "vigencia_inicio",
            "vigencia_fim",
            "valor_inicial",
            "valor_global",
            "valor_acumulado",
            "situacao_contrato",
        ],
        "contrato_id",
        "contrato_snapshot_v1",
    )
    fornecedores = correntes.select(
        "fornecedor_id",
        "tipo_pessoa",
        lit("BRA").alias("pais"),
        "identificador_publico",
        "nome_fornecedor",
        "atualizado_em",
        "source_file_id",
    ).dropDuplicates(["fornecedor_id"])
    contratos = correntes.drop(
        "identificador_normalizado",
        "identificador_publico",
        "chave_fornecedor",
        "tipo_pessoa",
        "nome_fornecedor",
    )
    conflitos = conflitos.drop("identificador_normalizado", "chave_fornecedor")
    return contratos, fornecedores, quarentena, conflitos


def transformar_itens_contrato(
    bronze: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    tipadas = (
        bronze.select(
            trim(_coluna_ou_nulo(bronze, "id")).alias("id_origem_item_contrato"),
            trim(_coluna_ou_nulo(bronze, "contrato_id")).alias(
                "id_origem_contrato"
            ),
            when(
                lower(_coluna_ou_nulo(bronze, "tipo_id")).contains("servi"), "S"
            )
            .when(
                lower(_coluna_ou_nulo(bronze, "tipo_id")).contains("material"),
                "M",
            )
            .alias("material_ou_servico"),
            concat_ws(
                " ",
                trim(_coluna_ou_nulo(bronze, "catmatseritem_id")),
                trim(_coluna_ou_nulo(bronze, "descricao_complementar")),
            ).alias("descricao"),
            trim(_coluna_ou_nulo(bronze, "grupo_id")).alias("grupo_origem"),
            _decimal(bronze, "quantidade", 6).alias("quantidade"),
            _decimal(bronze, "valorunitario", 4).alias("valor_unitario"),
            _decimal(bronze, "valortotal", 2).alias("valor_total"),
            trim(_coluna_ou_nulo(bronze, "numero_item_compra")).alias(
                "numero_item_compra"
            ),
            _timestamp_composto(bronze, "data_inicio_item").alias(
                "data_inicio_item"
            ),
            coalesce(
                try_to_timestamp(
                    _coluna_ou_nulo(bronze, "_data_publicacao_arquivo")
                ),
                try_to_timestamp(_coluna_ou_nulo(bronze, "_coletado_em_utc")),
            ).alias("atualizado_em"),
            col("_source_file_id").alias("source_file_id"),
        )
        .withColumn(
            "contrato_id",
            sha2(concat(lit("comprasnet_contratos|"), col("id_origem_contrato")), 256),
        )
        .withColumn(
            "contrato_item_id",
            sha2(
                concat(
                    lit("comprasnet_contratos|item|"), col("id_origem_item_contrato")
                ),
                256,
            ),
        )
        .withColumn(
            "motivo_quarentena",
            when(
                col("id_origem_item_contrato").isNull()
                | (col("id_origem_item_contrato") == ""),
                "id_item_ausente",
            )
            .when(
                col("id_origem_contrato").isNull()
                | (col("id_origem_contrato") == ""),
                "contrato_ausente",
            )
            .when(
                col("quantidade").isNull() | (col("quantidade") <= 0),
                "quantidade_nao_positiva",
            )
            .when(col("valor_unitario") < 0, "valor_unitario_negativo")
            .when(col("valor_total") < 0, "valor_total_negativo")
            .when(col("atualizado_em").isNull(), "observacao_invalida"),
        )
    )
    quarentena = tipadas.where(col("motivo_quarentena").isNotNull())
    validas = tipadas.where(col("motivo_quarentena").isNull()).drop(
        "motivo_quarentena"
    )
    correntes, conflitos = _separar_versoes(
        validas,
        ["contrato_item_id", "atualizado_em"],
        [
            "contrato_id",
            "material_ou_servico",
            "descricao",
            "grupo_origem",
            "quantidade",
            "valor_unitario",
            "valor_total",
            "numero_item_compra",
            "data_inicio_item",
        ],
        "contrato_item_id",
        "contrato_item_snapshot_v1",
    )
    correntes = classificar_servicos(classificar_equipamentos(correntes))
    return correntes, quarentena, conflitos


def transformar_eventos_contrato(
    bronze: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    tipadas = (
        bronze.select(
            trim(_coluna_ou_nulo(bronze, "id")).alias("id_origem_evento"),
            trim(_coluna_ou_nulo(bronze, "contrato_id")).alias(
                "id_origem_contrato"
            ),
            trim(_coluna_ou_nulo(bronze, "tipo")).alias("tipo_evento"),
            trim(_coluna_ou_nulo(bronze, "qualificacao_termo")).alias(
                "qualificacao_termo"
            ),
            trim(_coluna_ou_nulo(bronze, "observacao")).alias("observacao"),
            _date(bronze, "data_assinatura").alias(
                "data_assinatura"
            ),
            _date(bronze, "data_publicacao").alias(
                "data_publicacao"
            ),
            _date(bronze, "vigencia_inicio").alias(
                "vigencia_inicio"
            ),
            _date(bronze, "vigencia_fim").alias("vigencia_fim"),
            _decimal(bronze, "valor_inicial", 2).alias("valor_inicial"),
            _decimal(bronze, "valor_global", 2).alias("valor_global"),
            _decimal(bronze, "novo_valor_global", 2).alias("novo_valor_global"),
            trim(_coluna_ou_nulo(bronze, "situacao_contrato")).alias(
                "situacao_contrato"
            ),
            trim(_coluna_ou_nulo(bronze, "situacao_termo")).alias("situacao_termo"),
            coalesce(
                _timestamp_composto(bronze, "alterado_em"),
                _timestamp_composto(bronze, "criado_em"),
                try_to_timestamp(
                    _coluna_ou_nulo(bronze, "_data_publicacao_arquivo")
                ),
                try_to_timestamp(_coluna_ou_nulo(bronze, "_coletado_em_utc")),
            ).alias("atualizado_em"),
            col("_source_file_id").alias("source_file_id"),
        )
        .withColumn(
            "contrato_id",
            sha2(concat(lit("comprasnet_contratos|"), col("id_origem_contrato")), 256),
        )
        .withColumn(
            "evento_contrato_id",
            sha2(
                concat(lit("comprasnet_contratos|evento|"), col("id_origem_evento")),
                256,
            ),
        )
        .withColumn(
            "variacao_valor",
            col("novo_valor_global") - col("valor_global"),
        )
        .withColumn(
            "motivo_quarentena",
            when(
                col("id_origem_evento").isNull() | (col("id_origem_evento") == ""),
                "id_evento_ausente",
            )
            .when(
                col("id_origem_contrato").isNull()
                | (col("id_origem_contrato") == ""),
                "contrato_ausente",
            )
            .when(col("valor_global") < 0, "valor_global_negativo")
            .when(col("novo_valor_global") < 0, "novo_valor_global_negativo")
            .when(col("atualizado_em").isNull(), "data_evento_invalida"),
        )
    )
    quarentena = tipadas.where(col("motivo_quarentena").isNotNull())
    validas = tipadas.where(col("motivo_quarentena").isNull()).drop(
        "motivo_quarentena"
    )
    correntes, conflitos = _separar_versoes(
        validas,
        ["evento_contrato_id", "atualizado_em"],
        [
            "contrato_id",
            "tipo_evento",
            "qualificacao_termo",
            "observacao",
            "data_assinatura",
            "data_publicacao",
            "vigencia_inicio",
            "vigencia_fim",
            "valor_inicial",
            "valor_global",
            "novo_valor_global",
            "variacao_valor",
            "situacao_contrato",
            "situacao_termo",
        ],
        "evento_contrato_id",
        "evento_contrato_snapshot_v1",
    )
    return correntes, quarentena, conflitos


def _decimal(dados: DataFrame, nome: str, escala: int):
    return _coluna_ou_nulo(dados, nome).try_cast(DecimalType(38, escala))


def _date(dados: DataFrame, nome: str):
    return to_date(try_to_timestamp(_coluna_ou_nulo(dados, nome)))


def _timestamp_composto(dados: DataFrame, nome: str):
    valor = _coluna_ou_nulo(dados, nome)
    interno = regexp_extract(valor, r"'date': '([^']+)'", 1)
    return coalesce(try_to_timestamp(interno), try_to_timestamp(valor))
