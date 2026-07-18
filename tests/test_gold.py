import pytest
from pyspark.sql.functions import col, lit

from rastro_publico.transformacoes.contratacoes import transformar_contratacoes
from rastro_publico.transformacoes.gold import (
    avaliar_elegibilidade_monetaria,
    calcular_evolucao_contratual,
    calcular_cobertura_servicos,
    calcular_concentracao_fornecedores,
    calcular_presenca_fornecedores,
    calcular_kpis_compras,
    calcular_qualidade_cobertura,
    calcular_recorrencia_orgao_fornecedor,
    calcular_rede_orgao_fornecedor,
    calcular_variacao_precos,
    preparar_relacoes_gold,
)
from rastro_publico.transformacoes.nucleo import (
    classificar_equipamentos,
    classificar_servicos,
    transformar_itens,
    transformar_resultados,
    transformar_vinculos_contratacao,
)
from scripts.audit_corrected_kpis import auditar_compras_brutas


def entradas(spark):
    itens = spark.createDataFrame(
        [
            ("c1", "i1", "computador_notebook", "UN", 1.0, 100.0),
            ("c1", "i2", "computador_notebook", "UN", 1.0, 200.0),
            ("c1", "i3", "computador_notebook", "UN", 1.0, 300.0),
            ("c2", "i4", "monitor", None, 1.0, None),
            ("c3", "i5", "incerto", "UN", 1.0, 50.0),
        ],
        [
            "contratacao_id",
            "item_id",
            "categoria_tecnologia",
            "unidade_medida",
            "quantidade",
            "valor_unitario_estimado",
        ],
    )
    resultados = spark.createDataFrame(
        [
            ("r1", "i1", "f1", 2.0, 30.0, 60.0, False),
            ("r2", "i2", "f1", 1.0, 30.0, 30.0, False),
            ("r3", "i3", "f2", 1.0, 10.0, 10.0, False),
            ("r4", "i4", "f3", 1.0, 20.0, 20.0, True),
        ],
        [
            "resultado_id",
            "item_id",
            "fornecedor_id",
            "quantidade_homologada",
            "valor_unitario_homologado",
            "valor_total_homologado",
            "cancelado",
        ],
    )
    contratacoes = spark.createDataFrame(
        [
            ("c1", "2026-07-01", "6", "Pregao"),
            ("c2", "2026-07-02", "8", "Dispensa"),
            ("c3", None, None, None),
        ],
        ["contratacao_id", "publicado_em", "modalidade_id", "modalidade"],
    )
    vinculos = spark.createDataFrame(
        [("c1", "o1"), ("c2", "o2")],
        ["contratacao_id", "orgao_id"],
    )
    qualidade = spark.createDataFrame(
        [
            (
                "run-1",
                "registros_em_quarentena",
                2,
                100,
                2.0,
                "alerta",
                "ALERT",
                "2026-07-17",
            ),
            (
                "run-2",
                "registros_em_quarentena",
                3,
                120,
                2.5,
                "alerta",
                "ALERT",
                "2026-07-18",
            ),
        ],
        [
            "run_id",
            "regra",
            "observados",
            "total",
            "percentual",
            "severidade",
            "status",
            "registrado_em",
        ],
    )
    return itens, resultados, contratacoes, vinculos, qualidade


def test_qualidade_publica_coberturas_e_ultimo_resultado_operacional(spark) -> None:
    gold = calcular_qualidade_cobertura(*entradas(spark))

    notebook = gold.where("categoria_tecnologia = 'computador_notebook'").first()
    operacional = gold.where("metrica = 'registros_em_quarentena'").first()

    assert notebook.total_itens == 3
    assert notebook.itens_com_resultado == 3
    assert notebook.cobertura_fornecedor == 1.0
    assert notebook.cobertura_categoria == 1.0
    assert operacional.observados == 3
    assert operacional.total == 120


def test_concentracao_reconcilia_top1_top3_hhi_e_estado(spark) -> None:
    itens, resultados, contratacoes, vinculos, _ = entradas(spark)

    gold = calcular_concentracao_fornecedores(
        itens,
        resultados,
        contratacoes,
        vinculos,
        minimo_fornecedores=2,
        minimo_resultados=3,
        semantica_fonte_validada=True,
    ).first()

    assert gold.valor_total_homologado == pytest.approx(100.0)
    assert gold.fornecedores_distintos == 2
    assert gold.resultados_elegiveis == 3
    assert gold.top_1 == pytest.approx(0.9)
    assert float(gold.top_3) == pytest.approx(1.0)
    assert gold.hhi == pytest.approx(0.82)
    assert gold.status_publicacao == "publicada"


def test_concentracao_nao_publica_grupo_sem_populacao_minima(spark) -> None:
    itens, resultados, contratacoes, vinculos, _ = entradas(spark)

    gold = calcular_concentracao_fornecedores(
        itens,
        resultados,
        contratacoes,
        vinculos,
        minimo_fornecedores=3,
        minimo_resultados=3,
        semantica_fonte_validada=True,
    ).first()

    assert gold.status_publicacao == "nao_publicavel"
    assert gold.limitacao == "fornecedores_insuficientes"


def test_concentracao_limita_top3_apos_arredondamento_decimal(spark) -> None:
    itens = spark.createDataFrame(
        [
            ("c1", "i1", "computador_notebook"),
            ("c1", "i2", "computador_notebook"),
            ("c1", "i3", "computador_notebook"),
        ],
        ["contratacao_id", "item_id", "categoria_tecnologia"],
    )
    resultados = spark.createDataFrame(
        [
            ("r1", "i1", "f1", 1.0, 1.0, 1.0, False),
            ("r2", "i2", "f2", 1.0, 1.0, 1.0, False),
            ("r3", "i3", "f3", 1.0, 4.0, 4.0, False),
        ],
        [
            "resultado_id",
            "item_id",
            "fornecedor_id",
            "quantidade_homologada",
            "valor_unitario_homologado",
            "valor_total_homologado",
            "cancelado",
        ],
    ).withColumn(
        "valor_total_homologado",
        col("valor_total_homologado").cast("decimal(38,2)"),
    )
    contratacoes = spark.createDataFrame(
        [("c1", "2026-07-01", "6", "Pregao")],
        ["contratacao_id", "publicado_em", "modalidade_id", "modalidade"],
    )
    vinculos = spark.createDataFrame(
        [("c1", "o1")], ["contratacao_id", "orgao_id"]
    )

    gold = calcular_concentracao_fornecedores(
        itens,
        resultados,
        contratacoes,
        vinculos,
        semantica_fonte_validada=True,
    ).first()

    assert gold.top_3 == pytest.approx(1.0)
    assert gold.top_3 <= 1


def test_gate_monetario_aprova_total_coerente_e_reprova_inconsistente(spark) -> None:
    resultados = spark.createDataFrame(
        [
            ("r1", 2.0, 50.0, 100.0),
            ("r2", 2.0, 50.0, 130.0),
        ],
        [
            "resultado_id",
            "quantidade_homologada",
            "valor_unitario_homologado",
            "valor_total_homologado",
        ],
    )

    avaliados = {
        linha.resultado_id: linha
        for linha in avaliar_elegibilidade_monetaria(resultados).collect()
    }

    assert avaliados["r1"].elegivel_monetario is True
    assert avaliados["r2"].elegivel_monetario is False
    assert (
        avaliados["r2"].motivo_inelegibilidade_monetaria
        == "total_diverge_quantidade_vezes_unitario"
    )


def test_gate_monetario_reprova_duplicidade_do_resultado(spark) -> None:
    resultados = spark.createDataFrame(
        [("r1", 1.0, 100.0, 100.0), ("r1", 1.0, 100.0, 100.0)],
        [
            "resultado_id",
            "quantidade_homologada",
            "valor_unitario_homologado",
            "valor_total_homologado",
        ],
    )

    avaliados = avaliar_elegibilidade_monetaria(resultados)

    assert avaliados.where("elegivel_monetario").count() == 0
    assert {
        linha.motivo_inelegibilidade_monetaria for linha in avaliados.collect()
    } == {"resultado_duplicado"}


def test_concentracao_monetaria_fica_oculta_sem_validacao_da_fonte(spark) -> None:
    itens, resultados, contratacoes, vinculos, _ = entradas(spark)

    gold = calcular_concentracao_fornecedores(
        itens, resultados, contratacoes, vinculos
    ).first()

    assert gold.gate_monetario == "reprovado"
    assert gold.limitacao == "semantica_fonte_nao_validada"
    assert gold.top_1 is None
    assert gold.top_3 is None
    assert gold.hhi is None
    assert gold.valor_total_homologado is None
    assert gold.valor_observado is None
    assert gold.cobertura_valor is None


def test_concentracao_preserva_grupo_sem_resultado_monetario_elegivel(spark) -> None:
    itens, resultados, contratacoes, vinculos, _ = entradas(spark)
    resultados = resultados.withColumn("valor_total_homologado", lit(999.0))

    gold = calcular_concentracao_fornecedores(
        itens,
        resultados,
        contratacoes,
        vinculos,
        semantica_fonte_validada=True,
    ).first()

    assert gold is not None
    assert gold.resultados_elegiveis == 0
    assert gold.cobertura_semantica_valor == 0
    assert gold.status_publicacao == "nao_publicavel"
    assert gold.limitacao == "semantica_valor_inconsistente"


def test_cobertura_servicos_mantem_preco_nao_publicavel(spark) -> None:
    itens = spark.createDataFrame(
        [
            ("i1", "cloud", "UNIDADE", 100.0, "nao_publicavel"),
            ("i2", "cloud", "MÊS", 200.0, "nao_publicavel"),
            ("i3", "suporte", "HORA", None, "nao_publicavel"),
        ],
        [
            "item_id",
            "categoria_servico",
            "unidade_medida",
            "valor_unitario_estimado",
            "status_preco_servico",
        ],
    )

    cobertura = calcular_cobertura_servicos(itens)
    cloud = cobertura.where("categoria_servico = 'cloud'").first()

    assert cloud.total_itens == 2
    assert cloud.unidades_distintas == 2
    assert cloud.itens_com_preco == 2
    assert cloud.status_publicacao_preco == "nao_publicavel"


def test_recorrencia_presenca_e_rede_reconciliam_relacoes(spark) -> None:
    itens = spark.createDataFrame(
        [
            ("c1", "i1", "monitor"),
            ("c2", "i2", "monitor"),
            ("c3", "i3", "servidor"),
        ],
        ["contratacao_id", "item_id", "categoria_tecnologia"],
    )
    resultados = spark.createDataFrame(
        [
            ("r1", "i1", "c1", "f1", 100.0, False),
            ("r2", "i2", "c2", "f1", 150.0, False),
            ("r3", "i3", "c3", "f2", 300.0, False),
        ],
        [
            "resultado_id",
            "item_id",
            "contratacao_id",
            "fornecedor_id",
            "valor_total_homologado",
            "cancelado",
        ],
    )
    contratacoes = spark.createDataFrame(
        [
            ("c1", "2026-05-01", "6", "Pregao"),
            ("c2", "2026-06-01", "6", "Pregao"),
            ("c3", "2026-06-02", "8", "Dispensa"),
        ],
        ["contratacao_id", "publicado_em", "modalidade_id", "modalidade"],
    )
    vinculos = spark.createDataFrame(
        [("c1", "o1", "u1"), ("c2", "o1", "u1"), ("c3", "o2", "u2")],
        ["contratacao_id", "orgao_id", "unidade_id"],
    )
    unidades = spark.createDataFrame(
        [("u1", "RJ", "Rio de Janeiro"), ("u2", "SP", "Sao Paulo")],
        ["unidade_id", "uf", "municipio"],
    )

    recorrencias = calcular_recorrencia_orgao_fornecedor(
        itens, resultados, contratacoes, vinculos
    )
    recorrencia = recorrencias.where(
        "orgao_id = 'o1' AND fornecedor_id = 'f1'"
    ).first()
    presenca = calcular_presenca_fornecedores(
        itens, resultados, contratacoes, vinculos, unidades
    ).where("fornecedor_id = 'f1'").first()
    rede = calcular_rede_orgao_fornecedor(
        itens, resultados, contratacoes, vinculos
    ).where("orgao_id = 'o1' AND fornecedor_id = 'f1'").first()

    assert recorrencia.contratacoes_distintas == 2
    assert recorrencia.periodos_distintos == 2
    assert recorrencia.dias_entre_primeira_ultima == 31
    assert recorrencias.count() == 1
    assert recorrencias.where("fornecedor_id = 'f2'").count() == 0
    assert presenca.orgaos_distintos == 1
    assert presenca.periodos_distintos == 2
    assert recorrencia.valor_total_homologado is None
    assert presenca.valor_total_homologado is None
    assert rede.valor_total_homologado is None
    assert rede.resultados_distintos == 2
    assert rede.status_publicacao == "publicada"


def test_auditoria_bruta_e_fluxo_produtivo_geram_o_mesmo_kpi(spark) -> None:
    compras_raw = spark.createDataFrame(
        [
            (
                "c1",
                "controle-1",
                "123",
                "Compra 1",
                "100",
                "2026-06-01",
                "6",
                "Pregao",
                "2026-06-02",
                "true",
                "false",
                "Orgao",
                "F",
                "E",
                "10",
                "Unidade",
                "RJ",
                "Rio",
                "3304557",
                "comprasgov",
                "a1",
                "run-1",
                "2026-06-03",
            ),
            (
                "c2",
                "controle-2",
                "123",
                "Compra 2",
                "100",
                "2026-07-01",
                "6",
                "Pregao",
                "2026-07-02",
                "true",
                "false",
                "Orgao",
                "F",
                "E",
                "10",
                "Unidade",
                "RJ",
                "Rio",
                "3304557",
                "comprasgov",
                "a2",
                "run-1",
                "2026-07-03",
            ),
        ],
        [
            "id_compra",
            "numero_controle_PNCP",
            "orgao_entidade_cnpj",
            "objeto_compra",
            "valor_total_estimado",
            "data_publicacao_pncp",
            "modalidade_id_pncp",
            "modalidade_nome",
            "data_atualizacao_pncp",
            "ind_atual",
            "contratacao_excluida",
            "orgao_entidade_razao_social",
            "orgao_entidade_esfera_id",
            "orgao_entidade_poder_id",
            "unidade_orgao_codigo_unidade",
            "unidade_orgao_nome_unidade",
            "unidade_orgao_uf_sigla",
            "unidade_orgao_municipio_nome",
            "unidade_orgao_codigo_ibge",
            "_sistema_origem",
            "_source_file_id",
            "_run_id",
            "_coletado_em_utc",
        ],
    )
    itens_raw = spark.createDataFrame(
        [
            ("i1", "controle-1", "1", "Notebook", "", "M", "1", "UN", "100", "100", "2026-06-02", "a1"),
            ("i2", "controle-2", "1", "Notebook", "", "M", "1", "UN", "100", "100", "2026-07-02", "a2"),
        ],
        [
            "id_compra_item",
            "numero_controle_PNCP_compra",
            "numero_item_pncp",
            "descricao_resumida",
            "descricao_detalhada",
            "material_ou_servico",
            "quantidade",
            "unidade_medida",
            "valor_unitario_estimado",
            "valor_total",
            "data_atualizacao_pncp",
            "_source_file_id",
        ],
    )
    resultados_raw = spark.createDataFrame(
        [
            ("r1", "i1", "controle-1", "1", "1", "PJ", "BRA", "12345678000199", "Fornecedor", "1", "100", "100", "2026-06-02", None, None, "Homologado", "a1"),
            ("r2", "i2", "controle-2", "1", "1", "PJ", "BRA", "12345678000199", "Fornecedor", "1", "100", "100", "2026-07-02", None, None, "Homologado", "a2"),
            ("r3", "i2", "controle-2", "1", "2", "PJ", "BRA", "99999999000199", "Cancelado", "1", "50", "50", "2026-07-02", "2026-07-03", "Cancelado", "Cancelado", "a2"),
        ],
        [
            "srk_item_resultado",
            "id_compra_item",
            "numero_controle_PNCP_compra",
            "numero_item_pncp",
            "sequencial_resultado",
            "tipo_pessoa",
            "codigo_pais",
            "ni_fornecedor",
            "nome_razao_social_fornecedor",
            "quantidade_homologada",
            "valor_unitario_homologado",
            "valor_total_homologado",
            "data_atualizacao_pncp",
            "data_cancelamento_pncp",
            "motivo_cancelamento",
            "situacao_compra_item_resultado_nome",
            "_source_file_id",
        ],
    )

    contratacoes = transformar_contratacoes(compras_raw)[0]
    itens = classificar_servicos(
        classificar_equipamentos(transformar_itens(itens_raw)[0])
    ).join(contratacoes.select("contratacao_id"), "contratacao_id", "inner")
    resultados = transformar_resultados(resultados_raw, "segredo")[0].join(
        itens.select("item_id"), "item_id", "inner"
    )
    vinculos = transformar_vinculos_contratacao(compras_raw).join(
        contratacoes.select("contratacao_id"), "contratacao_id", "inner"
    )
    produtivo = calcular_kpis_compras(
        itens, resultados, contratacoes, vinculos
    ).first()
    auditado = auditar_compras_brutas(
        compras_raw, itens_raw, resultados_raw, "segredo"
    ).first()

    assert auditado.asDict() == produtivo.asDict()
    assert auditado.resultados_tecnologia == 2
    assert auditado.relacoes_recorrentes == 1


def test_gold_preserva_multiplos_resultados_do_item_e_exclui_cancelado(spark) -> None:
    itens = spark.createDataFrame(
        [("c1", "i1", "monitor")],
        ["contratacao_id", "item_id", "categoria_tecnologia"],
    )
    resultados = spark.createDataFrame(
        [
            ("r1", "i1", "f1", 100.0, False),
            ("r2", "i1", "f2", 90.0, False),
            ("r3", "i1", "f3", 80.0, True),
        ],
        [
            "resultado_id",
            "item_id",
            "fornecedor_id",
            "valor_total_homologado",
            "cancelado",
        ],
    )
    contratacoes = spark.createDataFrame(
        [("c1", "2026-07-01", "6", "Pregao")],
        ["contratacao_id", "publicado_em", "modalidade_id", "modalidade"],
    )
    vinculos = spark.createDataFrame(
        [("c1", "o1", "u1")], ["contratacao_id", "orgao_id", "unidade_id"]
    )

    base = preparar_relacoes_gold(itens, resultados, contratacoes, vinculos)

    assert {linha.resultado_id for linha in base.collect()} == {"r1", "r2"}


def test_preco_por_categoria_e_unidade_e_medido_mas_nao_publicado(spark) -> None:
    itens = spark.createDataFrame(
        [
            ("c1", "i1", "monitor", "UN", "M"),
            ("c1", "i2", "monitor", "UN", "M"),
            ("c1", "i3", "monitor", "UN", "M"),
        ],
        [
            "contratacao_id",
            "item_id",
            "categoria_tecnologia",
            "unidade_medida",
            "material_ou_servico",
        ],
    )
    resultados = spark.createDataFrame(
        [
            ("r1", "i1", "c1", 90.0, False),
            ("r2", "i2", "c1", 100.0, False),
            ("r3", "i3", "c1", 120.0, False),
        ],
        [
            "resultado_id",
            "item_id",
            "contratacao_id",
            "valor_unitario_homologado",
            "cancelado",
        ],
    )
    contratacoes = spark.createDataFrame(
        [("c1", "2026-06-01")], ["contratacao_id", "publicado_em"]
    )

    grupo = calcular_variacao_precos(itens, resultados, contratacoes).first()

    assert grupo.observacoes == 3
    assert grupo.mediana == pytest.approx(100.0)
    assert grupo.status_publicacao == "nao_publicavel"
    assert grupo.comparabilidade_avaliada is False
    assert grupo.limitacao == "comparabilidade_desabilitada_sem_especificacao_produto"


def test_evolucao_contratual_resume_eventos_e_extensao(spark) -> None:
    contratos = spark.createDataFrame(
        [("ct1", "o1", "f1", "2026-01-01", "2026-12-31", 1000.0, 1200.0)],
        [
            "contrato_id",
            "orgao_codigo",
            "fornecedor_id",
            "vigencia_inicio",
            "vigencia_fim",
            "valor_inicial",
            "valor_global",
        ],
    )
    eventos = spark.createDataFrame(
        [
            ("e1", "ct1", "Aditivo", "2026-06-01", "2027-03-31", 200.0),
            ("e2", "ct1", "Apostilamento", "2026-07-01", None, 50.0),
        ],
        [
            "evento_contrato_id",
            "contrato_id",
            "tipo_evento",
            "data_assinatura",
            "vigencia_fim",
            "variacao_valor",
        ],
    )

    linha = calcular_evolucao_contratual(contratos, eventos).first()

    assert linha.eventos_distintos == 2
    assert linha.variacao_valor_acumulada == pytest.approx(250.0)
    assert linha.extensao_vigencia_dias == 90
    assert linha.status_publicacao == "publicada"
