import json
from pathlib import Path

SCHEMA_EVIDENCIA = "rastro-publico-evidencia-v2"
LOGICA_PRODUTIVA = "silver_gold_compartilhada"


def metadados_evidencia(gerador: str, status: str) -> dict[str, str]:
    return {
        "schema": SCHEMA_EVIDENCIA,
        "gerador": gerador,
        "logica": LOGICA_PRODUTIVA,
        "status": status,
    }


def carregar_evidencia_validada(
    caminho: Path, gerador: str, status: str
) -> dict:
    if not caminho.exists():
        raise RuntimeError(f"evidencia ausente; execute {gerador}: {caminho}")
    evidencia = json.loads(caminho.read_text(encoding="utf-8"))
    esperado = metadados_evidencia(gerador, status)
    if evidencia.get("_meta") != esperado:
        raise RuntimeError(f"evidencia historica ou invalida: {caminho}")
    return evidencia
