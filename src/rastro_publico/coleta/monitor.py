from __future__ import annotations

import argparse
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from rastro_publico.coleta.pncp import ENDPOINT_PUBLICACAO


def monitorar(
    *,
    probe: Callable[[], tuple[int | None, str]],
    registrar: Callable[[str], None],
    dormir: Callable[[float], None] = time.sleep,
    intervalo: int = 300,
) -> int:
    while True:
        status, detalhe = probe()
        disponivel = status in {200, 204}
        momento = datetime.now(UTC).isoformat()
        registrar(f"{momento} status={status} detalhe={detalhe}"
                  f"{' DISPONIVEL' if disponivel else ''}")
        if disponivel:
            return status
        dormir(intervalo)


def probe_pncp(data: str, modalidade: int, timeout: int) -> tuple[int | None, str]:
    parametros = urlencode(
        {
            "dataInicial": data,
            "dataFinal": data,
            "codigoModalidadeContratacao": modalidade,
            "pagina": 1,
            "tamanhoPagina": 50,
        }
    )
    request = Request(
        f"{ENDPOINT_PUBLICACAO}?{parametros}",
        headers={"Accept": "application/json", "User-Agent": "RastroPublico-monitor/0.1"},
    )
    try:
        with urlopen(request, timeout=timeout) as resposta:
            resposta.read(1)
            return resposta.status, "resposta recebida"
    except HTTPError as erro:
        return erro.code, type(erro).__name__
    except (URLError, TimeoutError) as erro:
        return None, type(erro).__name__


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitora a API de consultas do PNCP")
    parser.add_argument("--data", default="20260115")
    parser.add_argument("--modalidade", type=int, default=6)
    parser.add_argument("--intervalo", type=int, default=300)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--log", type=Path, default=Path(r"D:\RastroPublico\data\monitor-pncp.log"))
    args = parser.parse_args()
    if args.intervalo < 60:
        parser.error("--intervalo deve ser de pelo menos 60 segundos")

    args.log.parent.mkdir(parents=True, exist_ok=True)

    def registrar(linha: str) -> None:
        with args.log.open("a", encoding="utf-8") as arquivo:
            arquivo.write(f"{linha}\n")

    monitorar(
        probe=lambda: probe_pncp(args.data, args.modalidade, args.timeout),
        registrar=registrar,
        intervalo=args.intervalo,
    )


if __name__ == "__main__":
    main()
