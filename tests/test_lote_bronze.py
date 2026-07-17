import json
from hashlib import sha256

import pytest

from rastro_publico.coleta.lote import preparar_lote


def criar_lote(tmp_path, *, payload=b'{"data":[{"id":"x"}]}', status=200):
    arquivo = None
    hash_payload = None
    if payload is not None:
        arquivo = "pagina-00001.json"
        (tmp_path / arquivo).write_bytes(payload)
        hash_payload = sha256(payload).hexdigest()
    manifesto = {
        "run_id": "run-1",
        "criado_em_utc": "2026-07-17T20:00:00+00:00",
        "respostas": [
            {
                "endpoint": "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao",
                "url_origem": "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao?pagina=1",
                "data_inicio_consulta": "20260716",
                "data_fim_consulta": "20260716",
                "modalidade": 6,
                "pagina": 1,
                "coletado_em_utc": "2026-07-17T20:00:01+00:00",
                "status_http": status,
                "hash_payload": hash_payload,
                "arquivo": arquivo,
                "tentativas": [
                    {"numero": 1, "status_http": 429, "duracao_ms": 10, "erro": "HTTPError"},
                    {"numero": 2, "status_http": status, "duracao_ms": 20, "erro": None},
                ],
            }
        ],
    }
    caminho = tmp_path / "manifesto.json"
    caminho.write_text(json.dumps(manifesto), encoding="utf-8")
    return caminho


def test_prepara_bronze_e_ops_sem_misturar_erros(tmp_path) -> None:
    lote = preparar_lote(criar_lote(tmp_path))

    assert len(lote.bronze) == 1
    assert lote.bronze[0]["payload"] == b'{"data":[{"id":"x"}]}'
    assert "status_http" not in lote.bronze[0]
    assert lote.runs[0]["status"] == "SUCESSO"
    assert [item["status_http"] for item in lote.requests] == [429, 200]


def test_rejeita_payload_com_hash_divergente(tmp_path) -> None:
    manifesto = criar_lote(tmp_path)
    (tmp_path / "pagina-00001.json").write_bytes(b"alterado")

    with pytest.raises(ValueError, match="hash divergente"):
        preparar_lote(manifesto)


def test_204_produz_ops_sem_linha_bronze(tmp_path) -> None:
    lote = preparar_lote(criar_lote(tmp_path, payload=None, status=204))

    assert lote.bronze == []
    assert lote.runs[0]["respostas_com_payload"] == 0
    assert lote.requests[-1]["status_http"] == 204


def test_rejeita_arquivo_fora_do_lote(tmp_path) -> None:
    manifesto = criar_lote(tmp_path)
    documento = json.loads(manifesto.read_text(encoding="utf-8"))
    documento["respostas"][0]["arquivo"] = "../segredo.json"
    manifesto.write_text(json.dumps(documento), encoding="utf-8")

    with pytest.raises(ValueError, match="arquivo invalido"):
        preparar_lote(manifesto)
