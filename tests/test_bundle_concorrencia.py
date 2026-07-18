from pathlib import Path


def test_duas_tentativas_de_carga_sao_serializadas_pelo_job() -> None:
    bundle = Path("databricks.yml").read_text(encoding="utf-8")
    bloco = bundle.split("ingestao_arquivo_bronze:", 1)[1].split(
        "pipeline_anual:", 1
    )[0]

    assert "max_concurrent_runs: 1" in bloco
    assert "queue:\n        enabled: true" in bloco
    assert "02_carregar_arquivo_bronze.py" in bloco


def test_limite_de_concorrencia_nao_e_apresentado_como_merge_atomico() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "execução manual\n  concorrente não é suportada" in readme
    assert "Escritas concorrentes de ingestão usam" not in readme
