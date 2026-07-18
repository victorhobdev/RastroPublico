from pathlib import Path


def test_package_exposes_version() -> None:
    import rastro_publico

    assert rastro_publico.__version__ == "0.1.0"


def test_recorte_anual_materializa_staging_sem_sobrescrever_bronze() -> None:
    raiz = Path(__file__).parents[1]
    preparacao = (raiz / "notebooks/10_preparar_bronze_janela.py").read_text(
        encoding="utf-8"
    )

    assert "workspace.staging" in preparacao
    assert "workspace.bronze" not in preparacao
    for nome in (
        "03_transformar_contratacoes.py",
        "04_transformar_nucleo.py",
        "07_transformar_contratos.py",
    ):
        conteudo = (raiz / "notebooks" / nome).read_text(encoding="utf-8")
        assert 'dbutils.widgets.text("input_schema"' in conteudo


def test_gold_publica_arestas_sem_alegar_analise_de_grafos() -> None:
    raiz = Path(__file__).parents[1]
    publicacao = (raiz / "notebooks/09_publicar_contexto_e_gold.py").read_text(
        encoding="utf-8"
    )

    assert '"arestas_orgao_fornecedor"' in publicacao
    assert '"rede_orgao_fornecedor"' not in publicacao
