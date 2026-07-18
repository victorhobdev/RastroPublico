import json
from hashlib import sha256
from io import BytesIO
from urllib.error import HTTPError

import pytest

from rastro_publico.coleta import monitor as monitor_mod
from rastro_publico.coleta.pncp import coletar_pagina, escrever_lote
from rastro_publico.coleta.monitor import monitorar, registrar_linha


class Resposta:
    def __init__(self, corpo: bytes, status: int = 200) -> None:
        self._corpo = BytesIO(corpo)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def read(self, tamanho: int = -1) -> bytes:
        return self._corpo.read(tamanho)


def test_coleta_pagina_50_e_preserva_payload_original() -> None:
    corpo = b'{"data":[{"numeroControlePNCP":"x"}],"totalPaginas":1}'
    urls = []

    def abrir(request, timeout):
        urls.append((request.full_url, timeout))
        return Resposta(corpo)

    resultado = coletar_pagina(
        data="20260716",
        modalidade=6,
        pagina=1,
        abrir=abrir,
        dormir=lambda _: None,
    )

    assert "pagina=1" in urls[0][0]
    assert "tamanhoPagina=50" in urls[0][0]
    assert resultado.payload == corpo
    assert resultado.status_http == 200
    assert resultado.tentativas == 1
    assert resultado.hash_payload == sha256(corpo).hexdigest()


def test_429_tem_retry_limitado_sem_retry_after() -> None:
    chamadas = 0
    esperas = []

    def abrir(request, timeout):
        nonlocal chamadas
        chamadas += 1
        if chamadas < 3:
            raise HTTPError(request.full_url, 429, "rate limit", {}, None)
        return Resposta(b'{"data":[],"totalPaginas":0}')

    resultado = coletar_pagina(
        data="20260716",
        modalidade=6,
        pagina=1,
        abrir=abrir,
        dormir=esperas.append,
        jitter=lambda: 0.0,
    )

    assert resultado.status_http == 200
    assert resultado.tentativas == 3
    assert esperas == [1.0, 2.0]


def test_204_e_sucesso_vazio_sem_payload_bronze() -> None:
    resultado = coletar_pagina(
        data="20260716",
        modalidade=6,
        pagina=1,
        abrir=lambda *_args, **_kwargs: Resposta(b"", status=204),
        dormir=lambda _: None,
    )

    assert resultado.status_http == 204
    assert resultado.payload is None
    assert resultado.hash_payload is None


def test_lote_escreve_payload_e_manifesto_reconciliaveis(tmp_path) -> None:
    corpo = b'{"data":[{"numeroControlePNCP":"x"}],"totalPaginas":1}'
    resultado = coletar_pagina(
        data="20260716",
        modalidade=6,
        pagina=1,
        abrir=lambda *_args, **_kwargs: Resposta(corpo),
        dormir=lambda _: None,
    )

    manifesto = escrever_lote(tmp_path, "run-123", [resultado])
    documento = json.loads(manifesto.read_text(encoding="utf-8"))
    payload = tmp_path / documento["respostas"][0]["arquivo"]

    assert payload.read_bytes() == corpo
    assert documento["run_id"] == "run-123"
    assert documento["respostas"][0]["hash_payload"] == sha256(corpo).hexdigest()
    assert documento["respostas"][0]["pagina"] == 1


def test_lote_nao_colide_mesma_pagina_de_datas_ou_modalidades_distintas(
    tmp_path,
) -> None:
    resultados = [
        coletar_pagina(
            data=data,
            modalidade=modalidade,
            pagina=1,
            abrir=lambda *_args, **_kwargs: Resposta(
                f'{{"data":"{data}","modalidade":{modalidade}}}'.encode()
            ),
            dormir=lambda _: None,
        )
        for data, modalidade in (("20260715", 6), ("20260716", 6), ("20260716", 8))
    ]

    manifesto = escrever_lote(tmp_path, "run-multiplo", resultados)
    arquivos = [
        item["arquivo"]
        for item in json.loads(manifesto.read_text(encoding="utf-8"))["respostas"]
    ]

    assert len(arquivos) == len(set(arquivos)) == 3
    assert len(list(tmp_path.glob("*.json"))) == 4


def test_falha_final_expoe_tentativas() -> None:
    def abrir(request, timeout):
        raise HTTPError(request.full_url, 500, "server error", {}, None)

    with pytest.raises(RuntimeError, match="apos 3 tentativas"):
        coletar_pagina(
            data="20260716",
            modalidade=6,
            pagina=1,
            abrir=abrir,
            dormir=lambda _: None,
            jitter=lambda: 0.0,
        )


def test_monitor_para_no_primeiro_sucesso() -> None:
    respostas = iter([(None, "timeout"), (200, "ok")])
    registros = []
    esperas = []

    status = monitorar(
        probe=lambda: next(respostas),
        registrar=registros.append,
        dormir=esperas.append,
        intervalo=300,
    )

    assert status == 200
    assert esperas == [300]
    assert len(registros) == 4
    assert "INICIANDO" in registros[0]
    assert "RESULTADO" in registros[1]
    assert "timeout" in registros[1]
    assert "INICIANDO" in registros[2]
    assert "RESULTADO" in registros[3]
    assert "DISPONIVEL" in registros[3]


def test_registro_vai_para_terminal_e_arquivo(tmp_path, capsys) -> None:
    log = tmp_path / "monitor.log"

    registrar_linha("2026-07-17T18:00:00-03:00 INICIANDO", log)

    assert capsys.readouterr().out == "2026-07-17T18:00:00-03:00 INICIANDO\n"
    assert log.read_text(encoding="utf-8") == "2026-07-17T18:00:00-03:00 INICIANDO\n"


def test_probe_confirma_resposta_http(monkeypatch) -> None:
    monkeypatch.setattr(monitor_mod, "urlopen", lambda *_args, **_kwargs: Resposta(b"{}"))

    assert monitor_mod.probe_pncp("20260115", 6, 30) == (200, "resposta recebida")


def test_probe_registra_erro_http(monkeypatch) -> None:
    def falhar(request, timeout):
        raise HTTPError(request.full_url, 503, "indisponivel", {}, None)

    monkeypatch.setattr(monitor_mod, "urlopen", falhar)

    assert monitor_mod.probe_pncp("20260115", 6, 30) == (503, "HTTPError")
