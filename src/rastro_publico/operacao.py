from datetime import date, timedelta


def janela_incremental(
    watermark: date, data_fim: date, sobreposicao_dias: int = 3
) -> tuple[date, date]:
    if sobreposicao_dias < 1:
        raise ValueError("sobreposicao deve ser positiva")
    if data_fim < watermark:
        raise ValueError("data final anterior ao watermark")
    return watermark - timedelta(days=sobreposicao_dias - 1), data_fim


def decidir_watermark(
    anterior: date | None, data_fim: date, modo: str, sucesso_integral: bool
) -> date | None:
    if modo not in {"bootstrap", "incremental", "reprocessamento"}:
        raise ValueError(f"modo invalido: {modo}")
    if not sucesso_integral or modo == "reprocessamento":
        return anterior
    if modo == "bootstrap":
        return anterior or data_fim
    return max(anterior, data_fim) if anterior else data_fim


def avaliar_regra(
    regra: str,
    observados: int,
    total: int,
    limite: int,
    severidade: str,
) -> dict[str, str | int | float]:
    if severidade not in {"erro", "alerta"}:
        raise ValueError(f"severidade invalida: {severidade}")
    excedeu = observados > limite
    return {
        "regra": regra,
        "severidade": severidade,
        "observados": observados,
        "total": total,
        "limite": limite,
        "percentual": round(100 * observados / total, 4) if total else 0.0,
        "status": "FAIL"
        if excedeu and severidade == "erro"
        else "ALERT"
        if excedeu
        else "PASS",
    }
