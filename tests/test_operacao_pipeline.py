from datetime import date

import pytest

from rastro_publico.operacao import avaliar_regra, decidir_watermark, janela_incremental


def test_janela_incremental_sobrepoe_tres_dias() -> None:
    assert janela_incremental(date(2026, 7, 15), date(2026, 7, 16), 3) == (
        date(2026, 7, 13),
        date(2026, 7, 16),
    )


def test_janela_rejeita_fim_anterior_ao_watermark() -> None:
    with pytest.raises(ValueError, match="anterior"):
        janela_incremental(date(2026, 7, 15), date(2026, 7, 14), 3)


@pytest.mark.parametrize("modo", ["reprocessamento", "bootstrap"])
def test_watermark_nao_avanca_fora_do_incremental(modo: str) -> None:
    assert decidir_watermark(date(2026, 7, 15), date(2026, 7, 16), modo, True) == date(
        2026, 7, 15
    )


def test_watermark_so_avanca_apos_incremental_integral() -> None:
    anterior = date(2026, 7, 15)

    assert (
        decidir_watermark(anterior, date(2026, 7, 16), "incremental", False) == anterior
    )
    assert decidir_watermark(anterior, date(2026, 7, 16), "incremental", True) == date(
        2026, 7, 16
    )


def test_regra_bloqueante_falha_e_cobertura_apenas_alerta() -> None:
    erro = avaliar_regra("duplicidade", 1, 100, 0, "erro")
    alerta = avaliar_regra("cobertura", 20, 100, 10, "alerta")

    assert erro["status"] == "FAIL"
    assert alerta["status"] == "ALERT"
    assert alerta["percentual"] == 20.0
