from __future__ import annotations

import json
import random
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ENDPOINT_PUBLICACAO = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"


@dataclass(frozen=True)
class Tentativa:
    numero: int
    status_http: int | None
    duracao_ms: int
    erro: str | None


@dataclass(frozen=True)
class ResultadoColeta:
    endpoint: str
    url_origem: str
    data: str
    modalidade: int
    pagina: int
    coletado_em_utc: str
    status_http: int
    payload: bytes | None
    hash_payload: str | None
    tentativas: int
    registros_tentativa: tuple[Tentativa, ...]


def coletar_pagina(
    *,
    data: str,
    modalidade: int,
    pagina: int,
    abrir: Callable = urlopen,
    dormir: Callable[[float], None] = time.sleep,
    jitter: Callable[[], float] = random.random,
    max_tentativas: int = 3,
    timeout: int = 30,
) -> ResultadoColeta:
    parametros = urlencode(
        {
            "dataInicial": data,
            "dataFinal": data,
            "codigoModalidadeContratacao": modalidade,
            "pagina": pagina,
            "tamanhoPagina": 50,
        }
    )
    url = f"{ENDPOINT_PUBLICACAO}?{parametros}"
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "RastroPublico/0.1"})
    registros: list[Tentativa] = []

    for numero in range(1, max_tentativas + 1):
        inicio = time.perf_counter()
        try:
            with abrir(request, timeout=timeout) as resposta:
                status = resposta.status
                corpo = resposta.read()
            registros.append(Tentativa(numero, status, _duracao_ms(inicio), None))
            payload = corpo if status != 204 else None
            return ResultadoColeta(
                endpoint=ENDPOINT_PUBLICACAO,
                url_origem=url,
                data=data,
                modalidade=modalidade,
                pagina=pagina,
                coletado_em_utc=datetime.now(UTC).isoformat(),
                status_http=status,
                payload=payload,
                hash_payload=sha256(payload).hexdigest() if payload is not None else None,
                tentativas=numero,
                registros_tentativa=tuple(registros),
            )
        except (HTTPError, URLError, TimeoutError) as erro:
            status = erro.code if isinstance(erro, HTTPError) else None
            registros.append(Tentativa(numero, status, _duracao_ms(inicio), type(erro).__name__))
            transitiva = status in {429, 500, 502, 503, 504} or status is None
            if numero == max_tentativas or not transitiva:
                raise RuntimeError(f"coleta falhou apos {numero} tentativas") from erro
            dormir(2 ** (numero - 1) + jitter())

    raise AssertionError("loop de tentativas terminou sem resultado")


def escrever_lote(
    diretorio: Path,
    run_id: str,
    resultados: list[ResultadoColeta],
) -> Path:
    diretorio.mkdir(parents=True, exist_ok=True)
    if any(diretorio.iterdir()):
        raise FileExistsError(f"diretorio de lote nao esta vazio: {diretorio}")
    respostas = []
    for resultado in resultados:
        arquivo = None
        if resultado.payload is not None:
            arquivo = f"pagina-{resultado.pagina:05d}.json"
            (diretorio / arquivo).write_bytes(resultado.payload)
        respostas.append(
            {
                "endpoint": resultado.endpoint,
                "url_origem": resultado.url_origem,
                "data_inicio_consulta": resultado.data,
                "data_fim_consulta": resultado.data,
                "modalidade": resultado.modalidade,
                "pagina": resultado.pagina,
                "coletado_em_utc": resultado.coletado_em_utc,
                "status_http": resultado.status_http,
                "hash_payload": resultado.hash_payload,
                "arquivo": arquivo,
                "tentativas": [asdict(tentativa) for tentativa in resultado.registros_tentativa],
            }
        )

    manifesto = diretorio / "manifesto.json"
    manifesto.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "criado_em_utc": datetime.now(UTC).isoformat(),
                "respostas": respostas,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return manifesto


def _duracao_ms(inicio: float) -> int:
    return round((time.perf_counter() - inicio) * 1000)
