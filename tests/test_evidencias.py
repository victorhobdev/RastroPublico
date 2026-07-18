import json

import pytest

from rastro_publico.evidencias import (
    carregar_evidencia_validada,
    metadados_evidencia,
)


def test_evidencia_exige_metadados_da_logica_produtiva(tmp_path) -> None:
    caminho = tmp_path / "evidencia.json"
    caminho.write_text(json.dumps({"resultado": 1}), encoding="utf-8")

    with pytest.raises(RuntimeError, match="historica ou invalida"):
        carregar_evidencia_validada(caminho, "audit_corrected_kpis", "VALIDADA")

    caminho.write_text(
        json.dumps(
            {
                "_meta": metadados_evidencia(
                    "audit_corrected_kpis", "VALIDADA"
                ),
                "resultado": 1,
            }
        ),
        encoding="utf-8",
    )

    assert carregar_evidencia_validada(
        caminho, "audit_corrected_kpis", "VALIDADA"
    )["resultado"] == 1
