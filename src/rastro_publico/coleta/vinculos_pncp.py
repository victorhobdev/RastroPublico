from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = "https://pncp.gov.br/api/pncp/v1"
PADRAO_CONTROLE = re.compile(r"^(\d{14})-\d+-(\d+)/(\d{4})$")


@dataclass(frozen=True)
class ControleContratacao:
    numero_controle_pncp: str
    cnpj: str
    ano: int
    sequencial: int


@dataclass(frozen=True)
class ChaveContrato:
    numero_controle_pncp: str
    cnpj: str
    ano: int
    sequencial: int


def extrair_controles_statement(documento: dict) -> list[ControleContratacao]:
    colunas = [
        coluna["name"] for coluna in documento["manifest"]["schema"]["columns"]
    ]
    indice = colunas.index("numero_controle_pncp")
    controles = {
        _interpretar_controle(linha[indice])
        for linha in documento["result"]["data_array"]
        if linha[indice]
    }
    return sorted(controles, key=lambda controle: controle.numero_controle_pncp)


def extrair_chaves_contrato(diretorio_vinculos: Path) -> list[ChaveContrato]:
    chaves = set()
    for arquivo in diretorio_vinculos.glob("vinculo-*-pagina-*.json"):
        documento = json.loads(arquivo.read_text(encoding="utf-8"))
        for contrato in documento.get("data", []):
            chaves.add(
                ChaveContrato(
                    contrato["numeroControlePNCP"],
                    contrato["orgaoEntidade"]["cnpj"],
                    int(contrato["anoContrato"]),
                    int(contrato["sequencialContrato"]),
                )
            )
    return sorted(chaves, key=lambda chave: chave.numero_controle_pncp)


def medir_vinculos(
    controles: list[ControleContratacao],
    destino: Path,
    run_id: str,
    *,
    abrir: Callable = urlopen,
    dormir: Callable[[float], None] = time.sleep,
    jitter: Callable[[], float] = random.random,
    intervalo: float = 0.1,
    max_tentativas: int = 4,
    timeout: int = 30,
) -> dict:
    destino.mkdir(parents=True, exist_ok=True)
    if any(destino.iterdir()):
        raise FileExistsError(f"diretorio de lote nao esta vazio: {destino}")

    respostas = []
    contratacoes_com_vinculo = 0
    contratos_encontrados = 0
    respostas_404 = 0
    respostas_422 = 0
    for indice, controle in enumerate(controles, start=1):
        pagina = 1
        encontrou = False
        while True:
            parametros = urlencode({"pagina": pagina, "tamanhoPagina": 50})
            endpoint = (
                f"{BASE_URL}/orgaos/{controle.cnpj}/contratos/contratacao/"
                f"{controle.ano}/{controle.sequencial}"
            )
            url = f"{endpoint}?{parametros}"
            status, corpo, tentativas = _consultar(
                url,
                abrir=abrir,
                dormir=dormir,
                jitter=jitter,
                max_tentativas=max_tentativas,
                timeout=timeout,
            )
            registro = {
                "numero_controle_pncp": controle.numero_controle_pncp,
                "endpoint": endpoint,
                "url_origem": url,
                "pagina": pagina,
                "status_http": status,
                "tentativas": tentativas,
                "coletado_em_utc": datetime.now(UTC).isoformat(),
                "arquivo": None,
                "hash_payload": None,
            }
            if status in {404, 422}:
                if status == 404:
                    respostas_404 += 1
                else:
                    respostas_422 += 1
                if corpo:
                    nome = f"vinculo-{indice:05d}-erro-{status}.json"
                    (destino / nome).write_bytes(corpo)
                    registro["arquivo"] = nome
                    registro["hash_payload"] = sha256(corpo).hexdigest()
                respostas.append(registro)
                break

            assert corpo is not None
            nome = f"vinculo-{indice:05d}-pagina-{pagina:03d}.json"
            (destino / nome).write_bytes(corpo)
            registro["arquivo"] = nome
            registro["hash_payload"] = sha256(corpo).hexdigest()
            respostas.append(registro)

            documento = json.loads(corpo)
            dados = documento.get("data", [])
            contratos_encontrados += len(dados)
            encontrou = encontrou or bool(dados)
            total_paginas = max(int(documento.get("totalPaginas") or 1), 1)
            if pagina >= total_paginas:
                break
            pagina += 1
            dormir(intervalo)
        if encontrou:
            contratacoes_com_vinculo += 1
        if indice < len(controles):
            dormir(intervalo)

    resumo = {
        "run_id": run_id,
        "contratacoes_avaliadas": len(controles),
        "contratacoes_com_vinculo": contratacoes_com_vinculo,
        "contratos_encontrados": contratos_encontrados,
        "respostas_404": respostas_404,
        "respostas_422": respostas_422,
        "taxa_vinculo": contratacoes_com_vinculo / len(controles)
        if controles
        else 0.0,
    }
    (destino / "manifesto.json").write_text(
        json.dumps(
            {
                **resumo,
                "criado_em_utc": datetime.now(UTC).isoformat(),
                "controles": [asdict(controle) for controle in controles],
                "respostas": respostas,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return resumo


def medir_capacidades_contratuais(
    chaves: list[ChaveContrato],
    destino: Path,
    run_id: str,
    *,
    abrir: Callable = urlopen,
    dormir: Callable[[float], None] = time.sleep,
    jitter: Callable[[], float] = random.random,
    intervalo: float = 0.1,
    max_tentativas: int = 4,
    timeout: int = 30,
) -> dict:
    destino.mkdir(parents=True, exist_ok=True)
    if any(destino.iterdir()):
        raise FileExistsError(f"diretorio de lote nao esta vazio: {destino}")

    respostas = []
    contagens = {
        "detalhes_disponiveis": 0,
        "contratos_com_historico": 0,
        "eventos_historicos": 0,
        "contratos_com_termos": 0,
        "termos_encontrados": 0,
    }
    for indice, chave in enumerate(chaves, start=1):
        base = f"{BASE_URL}/orgaos/{chave.cnpj}/contratos/{chave.ano}/{chave.sequencial}"
        status, corpo, tentativas = _consultar(
            base,
            abrir=abrir,
            dormir=dormir,
            jitter=jitter,
            max_tentativas=max_tentativas,
            timeout=timeout,
        )
        respostas.append(
            _preservar_resposta(
                destino,
                indice,
                "detalhe",
                1,
                chave,
                base,
                status,
                corpo,
                tentativas,
            )
        )
        if status == 200:
            contagens["detalhes_disponiveis"] += 1
        dormir(intervalo)

        for capacidade, campo_contratos, campo_registros in (
            ("historico", "contratos_com_historico", "eventos_historicos"),
            ("termos", "contratos_com_termos", "termos_encontrados"),
        ):
            pagina = 1
            total_contrato = 0
            while True:
                url = f"{base}/{capacidade}?{urlencode({'pagina': pagina, 'tamanhoPagina': 50})}"
                status, corpo, tentativas = _consultar(
                    url,
                    abrir=abrir,
                    dormir=dormir,
                    jitter=jitter,
                    max_tentativas=max_tentativas,
                    timeout=timeout,
                )
                respostas.append(
                    _preservar_resposta(
                        destino,
                        indice,
                        capacidade,
                        pagina,
                        chave,
                        url,
                        status,
                        corpo,
                        tentativas,
                    )
                )
                if status != 200 or not corpo:
                    break
                documento = json.loads(corpo)
                registros = documento.get("data", []) if isinstance(documento, dict) else documento
                total_contrato += len(registros)
                if not registros:
                    break
                pagina += 1
                dormir(intervalo)
            if total_contrato:
                contagens[campo_contratos] += 1
                contagens[campo_registros] += total_contrato
            dormir(intervalo)

    resumo = {"run_id": run_id, "contratos_avaliados": len(chaves), **contagens}
    (destino / "manifesto.json").write_text(
        json.dumps(
            {
                **resumo,
                "criado_em_utc": datetime.now(UTC).isoformat(),
                "contratos": [asdict(chave) for chave in chaves],
                "respostas": respostas,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return resumo


def preparar_manifestos_bronze(
    caminhos_manifesto: list[Path],
) -> tuple[list[dict], list[dict]]:
    bronze = []
    operacao = []
    for caminho in caminhos_manifesto:
        documento = json.loads(caminho.read_text(encoding="utf-8"))
        run_id = documento["run_id"]
        for resposta in documento["respostas"]:
            capacidade = resposta.get("capacidade", "vinculo")
            endpoint = resposta.get("endpoint") or resposta["url_origem"].partition("?")[0]
            chave = (
                f"{run_id}|{capacidade}|{resposta['numero_controle_pncp']}|"
                f"{resposta['pagina']}|{resposta['url_origem']}"
            )
            request_id = sha256(chave.encode()).hexdigest()
            operacao.append(
                {
                    "request_id": request_id,
                    "run_id": run_id,
                    "capacidade": capacidade,
                    "numero_controle_pncp": resposta["numero_controle_pncp"],
                    "endpoint": endpoint,
                    "url_origem": resposta["url_origem"],
                    "pagina": resposta["pagina"],
                    "status_http": resposta["status_http"],
                    "tentativas": resposta["tentativas"],
                    "coletado_em_utc": resposta["coletado_em_utc"],
                }
            )
            arquivo = resposta["arquivo"]
            if resposta["status_http"] != 200 or not arquivo:
                continue
            payload = (caminho.parent / arquivo).read_bytes()
            hash_observado = sha256(payload).hexdigest()
            if hash_observado != resposta["hash_payload"]:
                raise ValueError(f"hash divergente: {arquivo}")
            bronze.append(
                {
                    "observacao_id": request_id,
                    "run_id": run_id,
                    "capacidade": capacidade,
                    "numero_controle_pncp": resposta["numero_controle_pncp"],
                    "endpoint": endpoint,
                    "url_origem": resposta["url_origem"],
                    "pagina": resposta["pagina"],
                    "coletado_em_utc": resposta["coletado_em_utc"],
                    "hash_payload": hash_observado,
                    "payload": payload,
                }
            )
    return bronze, operacao


def _preservar_resposta(
    destino: Path,
    indice: int,
    capacidade: str,
    pagina: int,
    chave: ChaveContrato,
    url: str,
    status: int,
    corpo: bytes | None,
    tentativas: int,
) -> dict:
    nome = None
    hash_payload = None
    if corpo:
        nome = f"contrato-{indice:04d}-{capacidade}-pagina-{pagina:03d}.json"
        (destino / nome).write_bytes(corpo)
        hash_payload = sha256(corpo).hexdigest()
    return {
        "numero_controle_pncp": chave.numero_controle_pncp,
        "capacidade": capacidade,
        "pagina": pagina,
        "url_origem": url,
        "status_http": status,
        "tentativas": tentativas,
        "coletado_em_utc": datetime.now(UTC).isoformat(),
        "arquivo": nome,
        "hash_payload": hash_payload,
    }


def _interpretar_controle(numero_controle: str) -> ControleContratacao:
    correspondencia = PADRAO_CONTROLE.fullmatch(numero_controle)
    if not correspondencia:
        raise ValueError(f"numero de controle PNCP invalido: {numero_controle}")
    cnpj, sequencial, ano = correspondencia.groups()
    return ControleContratacao(numero_controle, cnpj, int(ano), int(sequencial))


def _consultar(
    url: str,
    *,
    abrir: Callable,
    dormir: Callable[[float], None],
    jitter: Callable[[], float],
    max_tentativas: int,
    timeout: int,
) -> tuple[int, bytes | None, int]:
    request = Request(
        url, headers={"Accept": "application/json", "User-Agent": "RastroPublico/0.1"}
    )
    for tentativa in range(1, max_tentativas + 1):
        try:
            with abrir(request, timeout=timeout) as resposta:
                return resposta.status, resposta.read(), tentativa
        except HTTPError as erro:
            if erro.code == 404:
                return 404, erro.read() or None, tentativa
            if erro.code == 422:
                return 422, erro.read() or None, tentativa
            transitiva = erro.code in {429, 500, 502, 503, 504}
            if not transitiva or tentativa == max_tentativas:
                raise RuntimeError(f"consulta falhou: HTTP {erro.code} em {url}") from erro
        except (URLError, TimeoutError) as erro:
            if tentativa == max_tentativas:
                raise RuntimeError(f"consulta falhou sem resposta em {url}") from erro
        dormir(2 ** (tentativa - 1) + jitter())
    raise AssertionError("loop de tentativas terminou sem resultado")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--destino", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--intervalo", type=float, default=0.1)
    parser.add_argument("--vinculos", type=Path)
    argumentos = parser.parse_args()
    if argumentos.vinculos:
        resumo = medir_capacidades_contratuais(
            extrair_chaves_contrato(argumentos.vinculos),
            argumentos.destino,
            argumentos.run_id,
            intervalo=argumentos.intervalo,
        )
        print(json.dumps(resumo, ensure_ascii=False))
        return
    documento = json.load(sys.stdin)
    controles = extrair_controles_statement(documento)
    resumo = medir_vinculos(
        controles,
        argumentos.destino,
        argumentos.run_id,
        intervalo=argumentos.intervalo,
    )
    print(json.dumps(resumo, ensure_ascii=False))


if __name__ == "__main__":
    main()
