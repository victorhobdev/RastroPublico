import hashlib
import hmac

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    concat,
    concat_ws,
    countDistinct,
    lit,
    lower,
    regexp_replace,
    row_number,
    sha2,
    struct,
    to_json,
    to_timestamp,
    translate,
    trim,
    udf,
    when,
)
from pyspark.sql.types import DecimalType, StringType


def pseudonimizar_identificador(segredo: str, valor: str) -> str:
    return hmac.new(segredo.encode(), valor.encode(), hashlib.sha256).hexdigest()


def transformar_itens(bronze: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    tipadas = (
        bronze.select(
            trim("id_compra_item").alias("id_origem_item"),
            trim("numero_controle_PNCP_compra").alias("numero_controle_pncp"),
            trim("numero_item_pncp").alias("numero_item"),
            concat_ws(
                " ", trim("descricao_resumida"), trim("descricao_detalhada")
            ).alias("descricao"),
            trim("material_ou_servico").alias("material_ou_servico"),
            col("quantidade").cast(DecimalType(38, 6)).alias("quantidade"),
            trim("unidade_medida").alias("unidade_medida"),
            col("valor_unitario_estimado")
            .cast(DecimalType(38, 4))
            .alias("valor_unitario_estimado"),
            col("valor_total").cast(DecimalType(38, 2)).alias("valor_total"),
            to_timestamp("data_atualizacao_pncp").alias("atualizado_em"),
            col("_source_file_id").alias("source_file_id"),
        )
        .withColumn(
            "contratacao_id",
            sha2(concat(lit("pncp|"), lower("numero_controle_pncp")), 256),
        )
        .withColumn(
            "item_id",
            sha2(concat_ws("|", "contratacao_id", "numero_item"), 256),
        )
        .withColumn(
            "motivo_quarentena",
            when(col("numero_controle_pncp").isNull(), "contratacao_ausente")
            .when(col("numero_item").isNull(), "numero_item_ausente")
            .when(
                col("quantidade").isNull() | (col("quantidade") <= 0),
                "quantidade_nao_positiva",
            )
            .when(
                col("unidade_medida").isNull() | (col("unidade_medida") == ""),
                "unidade_ausente",
            )
            .when(col("valor_unitario_estimado") < 0, "valor_unitario_negativo")
            .when(col("valor_total") < 0, "valor_total_negativo")
            .when(col("atualizado_em").isNull(), "data_atualizacao_invalida"),
        )
    )
    quarentena = tipadas.where(col("motivo_quarentena").isNotNull())
    validas = tipadas.where(col("motivo_quarentena").isNull()).drop("motivo_quarentena")
    correntes, conflitos = _separar_versoes(
        validas,
        ["item_id", "atualizado_em"],
        [
            "numero_controle_pncp",
            "numero_item",
            "descricao",
            "material_ou_servico",
            "quantidade",
            "unidade_medida",
            "valor_unitario_estimado",
            "valor_total",
            "atualizado_em",
        ],
        "item_id",
    )
    return correntes, quarentena, conflitos


def transformar_resultados(
    bronze: DataFrame, segredo: str
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    pseudonimizar = udf(
        lambda valor: pseudonimizar_identificador(segredo, valor) if valor else None,
        StringType(),
    )
    tipadas = (
        bronze.select(
            trim("srk_item_resultado").alias("id_origem_resultado"),
            trim("id_compra_item").alias("id_origem_item"),
            trim("numero_controle_PNCP_compra").alias("numero_controle_pncp"),
            trim("numero_item_pncp").alias("numero_item"),
            trim("sequencial_resultado").alias("sequencial_resultado"),
            trim("tipo_pessoa").alias("tipo_pessoa"),
            trim("codigo_pais").alias("pais"),
            regexp_replace("ni_fornecedor", r"\D", "").alias(
                "identificador_normalizado"
            ),
            trim("nome_razao_social_fornecedor").alias("nome_fornecedor"),
            col("quantidade_homologada")
            .cast(DecimalType(38, 6))
            .alias("quantidade_homologada"),
            col("valor_unitario_homologado")
            .cast(DecimalType(38, 4))
            .alias("valor_unitario_homologado"),
            col("valor_total_homologado")
            .cast(DecimalType(38, 2))
            .alias("valor_total_homologado"),
            to_timestamp(_coluna_ou_nulo(bronze, "data_cancelamento_pncp")).alias(
                "cancelado_em"
            ),
            trim(_coluna_ou_nulo(bronze, "motivo_cancelamento")).alias(
                "motivo_cancelamento"
            ),
            trim(_coluna_ou_nulo(bronze, "situacao_compra_item_resultado_nome")).alias(
                "situacao_resultado"
            ),
            to_timestamp("data_atualizacao_pncp").alias("atualizado_em"),
            col("_source_file_id").alias("source_file_id"),
        )
        .withColumn(
            "cancelado",
            col("cancelado_em").isNotNull()
            | lower("situacao_resultado").contains("cancel"),
        )
        .withColumn(
            "contratacao_id",
            sha2(concat(lit("pncp|"), lower("numero_controle_pncp")), 256),
        )
        .withColumn(
            "item_id", sha2(concat_ws("|", "contratacao_id", "numero_item"), 256)
        )
        .withColumn(
            "resultado_id",
            sha2(concat_ws("|", "item_id", "sequencial_resultado"), 256),
        )
        .withColumn(
            "chave_fornecedor",
            concat_ws("|", "tipo_pessoa", "pais", "identificador_normalizado"),
        )
        .withColumn(
            "fornecedor_id",
            when(
                col("tipo_pessoa") == "PF", pseudonimizar("chave_fornecedor")
            ).otherwise(sha2("chave_fornecedor", 256)),
        )
        .withColumn(
            "identificador_publico",
            when(col("tipo_pessoa") != "PF", col("identificador_normalizado")),
        )
        .withColumn(
            "motivo_quarentena",
            when(col("item_id").isNull(), "item_ausente")
            .when(
                col("identificador_normalizado").isNull()
                | (col("identificador_normalizado") == ""),
                "fornecedor_ausente",
            )
            .when(col("quantidade_homologada") < 0, "quantidade_negativa")
            .when(col("valor_unitario_homologado") < 0, "valor_unitario_negativo")
            .when(col("valor_total_homologado") < 0, "valor_total_negativo")
            .when(col("atualizado_em").isNull(), "data_atualizacao_invalida"),
        )
    )
    quarentena = tipadas.where(col("motivo_quarentena").isNotNull())
    validas = tipadas.where(col("motivo_quarentena").isNull()).drop("motivo_quarentena")
    correntes, conflitos = _separar_versoes(
        validas,
        ["resultado_id", "atualizado_em"],
        [
            "item_id",
            "sequencial_resultado",
            "fornecedor_id",
            "quantidade_homologada",
            "valor_unitario_homologado",
            "valor_total_homologado",
            "cancelado_em",
            "motivo_cancelamento",
            "situacao_resultado",
            "cancelado",
            "atualizado_em",
        ],
        "resultado_id",
    )
    resultados = correntes.drop(
        "identificador_normalizado", "identificador_publico", "chave_fornecedor"
    )
    fornecedores = correntes.select(
        "fornecedor_id",
        "tipo_pessoa",
        "pais",
        "identificador_publico",
        "nome_fornecedor",
        "atualizado_em",
        "source_file_id",
    ).dropDuplicates(["fornecedor_id"])
    campos_sensiveis = ["identificador_normalizado", "chave_fornecedor"]
    quarentena = quarentena.drop(*campos_sensiveis)
    conflitos = conflitos.drop(*campos_sensiveis)
    return resultados, fornecedores, quarentena, conflitos


def transformar_dimensoes(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    base = bronze.select(
        trim("orgao_entidade_cnpj").alias("cnpj_orgao"),
        trim("orgao_entidade_razao_social").alias("nome_orgao"),
        trim("orgao_entidade_esfera_id").alias("esfera"),
        trim("orgao_entidade_poder_id").alias("poder"),
        trim("unidade_orgao_codigo_unidade").alias("codigo_unidade"),
        trim("unidade_orgao_nome_unidade").alias("nome_unidade"),
        trim("unidade_orgao_uf_sigla").alias("uf"),
        trim("unidade_orgao_municipio_nome").alias("municipio"),
        trim("unidade_orgao_codigo_ibge").alias("codigo_ibge"),
        to_timestamp("data_atualizacao_pncp").alias("atualizado_em"),
        col("_source_file_id").alias("source_file_id"),
    ).where(col("cnpj_orgao").isNotNull() & (col("cnpj_orgao") != ""))

    orgaos = _mais_recente(
        base.select(
            sha2(concat(lit("orgao|"), "cnpj_orgao"), 256).alias("orgao_id"),
            "cnpj_orgao",
            "nome_orgao",
            "esfera",
            "poder",
            "atualizado_em",
            "source_file_id",
        ),
        "orgao_id",
    )
    unidades = _mais_recente(
        base.where(
            col("codigo_unidade").isNotNull() & (col("codigo_unidade") != "")
        ).select(
            sha2(concat(lit("orgao|"), "cnpj_orgao"), 256).alias("orgao_id"),
            sha2(concat_ws("|", "cnpj_orgao", "codigo_unidade"), 256).alias(
                "unidade_id"
            ),
            "codigo_unidade",
            "nome_unidade",
            "uf",
            "municipio",
            "codigo_ibge",
            "atualizado_em",
            "source_file_id",
        ),
        "unidade_id",
    )
    return orgaos, unidades


def transformar_vinculos_contratacao(bronze: DataFrame) -> DataFrame:
    vinculos = (
        bronze.select(
            trim("numero_controle_PNCP").alias("numero_controle_pncp"),
            trim("orgao_entidade_cnpj").alias("cnpj_orgao"),
            trim("unidade_orgao_codigo_unidade").alias("codigo_unidade"),
            to_timestamp("data_atualizacao_pncp").alias("atualizado_em"),
            col("_source_file_id").alias("source_file_id"),
        )
        .where(
            col("numero_controle_pncp").isNotNull()
            & (col("numero_controle_pncp") != "")
            & col("cnpj_orgao").isNotNull()
            & (col("cnpj_orgao") != "")
            & col("codigo_unidade").isNotNull()
            & (col("codigo_unidade") != "")
        )
        .select(
            sha2(concat(lit("pncp|"), lower("numero_controle_pncp")), 256).alias(
                "contratacao_id"
            ),
            sha2(concat(lit("orgao|"), "cnpj_orgao"), 256).alias("orgao_id"),
            sha2(concat_ws("|", "cnpj_orgao", "codigo_unidade"), 256).alias(
                "unidade_id"
            ),
            "atualizado_em",
            "source_file_id",
        )
    )
    return _mais_recente(vinculos, "contratacao_id")


def classificar_equipamentos(itens: DataFrame) -> DataFrame:
    texto = lower(col("descricao"))
    material = col("material_ou_servico") == "M"
    categoria = (
        when(
            material
            & texto.rlike(r"^(notebook|computador\b|desktop\b|microcomputador\b)"),
            "computador_notebook",
        )
        .when(
            material
            & texto.rlike(r"^monitor\s+(de\s+vídeo|vídeo|imagem|lcd|led|computador)"),
            "monitor",
        )
        .when(
            material & texto.rlike(r"^(impressora|scanner|multifuncional)\b"),
            "impressora_scanner",
        )
        .when(material & texto.rlike(r"^servidor\b"), "servidor")
        .when(
            material & texto.rlike(r"^(switch|roteador|access point|comutador)\b"),
            "equipamento_rede",
        )
        .otherwise("incerto")
    )
    return itens.withColumn("categoria_tecnologia", categoria).withColumn(
        "versao_regra", lit("equipamentos_v2")
    )


def classificar_servicos(itens: DataFrame) -> DataFrame:
    texto = regexp_replace(
        translate(
            lower(trim(col("descricao"))),
            "áàâãäéèêëíìîïóòôõöúùûüç",
            "aaaaaeeeeiiiiooooouuuuc",
        ),
        r"\s+",
        " ",
    )
    servico = col("material_ou_servico") == "S"
    categoria = (
        when(
            servico
            & texto.rlike(
                r"^(software como servico\s*-?\s*saas|servicos? de computacao "
                r"em nuvem|computacao em nuvem|infraestrutura como servico|"
                r"plataforma como servico|servicos? em nuvem|servicos? "
                r"especializados de disponibilizacao de copias de seguranca "
                r"de dados)\b"
            ),
            "cloud",
        )
        .when(
            servico
            & texto.rlike(
                r"^(cessao temporaria de direitos sobre programas? de computador|"
                r"licenciamento de direitos|licenca(?:mento)? de (?:uso de )?"
                r"(?:software|programas? de computador)|subscricao de software)\b"
            ),
            "licenciamento",
        )
        .when(
            servico
            & texto.rlike(
                r"^(desenvolvimento de novo software|servicos? de desenvolvimento "
                r"de (?:software|sistemas?|aplicativos?)|fabrica de software)\b"
            ),
            "desenvolvimento",
        )
        .when(
            servico
            & texto.rlike(
                r"^outsourcing (?:de impressao|de ti|de tecnologia|"
                r"de infraestrutura|de servicos?)\b"
            ),
            "outsourcing",
        )
        .when(
            servico
            & texto.rlike(
                r"^(servicos? de suporte tecnico (?:de|em) (?:tecnologia da "
                r"informacao|tic|informatica)|suporte tecnico (?:de|em) "
                r"(?:tecnologia da informacao|tic|informatica)|manutencao "
                r"(?:evolutiva )?de software|servicos? de manutencao e reparacao "
                r"de computadores)\b"
            ),
            "suporte",
        )
        .when(
            servico
            & texto.rlike(
                r"^(outros servicos para a infraestrutura de tecnologia da "
                r"informacao e comunicacao|servicos? de gerenciamento de "
                r"infraestrutura de tecnologia da ?informacao|servicos? de "
                r"hospedagem de sistemas|servicos? de data ?center|servico de "
                r"infraestrutura de redes? de comunicacao de dados|instalacao de "
                r"cabeamento estruturado|servico de instalacao.{0,80}manutencao "
                r"de rede local de computadores)\b"
            ),
            "infraestrutura",
        )
        .when(servico, "incerto")
    )
    return (
        itens.withColumn("categoria_servico", categoria)
        .withColumn(
            "categoria_tecnologia",
            when(
                col("categoria_servico").isNotNull()
                & (col("categoria_servico") != "incerto"),
                concat(lit("servico_"), col("categoria_servico")),
            ).otherwise(col("categoria_tecnologia")),
        )
        .withColumn(
            "status_preco_servico",
            when(
                col("categoria_servico").isNotNull()
                & (col("categoria_servico") != "incerto"),
                "nao_publicavel",
            ).when(servico, "fora_escopo"),
        )
        .withColumn(
            "motivo_preco_servico",
            when(
                col("status_preco_servico") == "nao_publicavel",
                "escopo_unidade_sla_nao_estruturados",
            ).when(servico, "familia_nao_identificada"),
        )
        .withColumn("versao_regra_servico", lit("servicos_v1"))
    )


def _separar_versoes(
    validas: DataFrame,
    chaves_conflito: list[str],
    campos_conteudo: list[str],
    chave_entidade: str,
) -> tuple[DataFrame, DataFrame]:
    com_hash = validas.withColumn(
        "hash_conteudo_entidade",
        sha2(
            to_json(struct(*campos_conteudo), options={"ignoreNullFields": "false"}),
            256,
        ),
    )
    empates = (
        com_hash.groupBy(*chaves_conflito)
        .agg(countDistinct("hash_conteudo_entidade").alias("total_hashes"))
        .where("total_hashes > 1")
        .drop("total_hashes")
    )
    conflitos = com_hash.join(empates, chaves_conflito, "inner")
    elegiveis = com_hash.join(empates, chaves_conflito, "left_anti")
    janela = Window.partitionBy(chave_entidade).orderBy(
        col("atualizado_em").desc(), col("source_file_id").desc()
    )
    correntes = (
        elegiveis.withColumn("_ordem", row_number().over(janela))
        .where("_ordem = 1")
        .drop("_ordem")
    )
    return correntes, conflitos


def _mais_recente(dados: DataFrame, chave: str) -> DataFrame:
    janela = Window.partitionBy(chave).orderBy(
        col("atualizado_em").desc(), col("source_file_id").desc()
    )
    return (
        dados.withColumn("_ordem", row_number().over(janela))
        .where("_ordem = 1")
        .drop("_ordem")
    )


def _coluna_ou_nulo(dados: DataFrame, nome: str):
    return col(nome) if nome in dados.columns else lit(None).cast("string")
