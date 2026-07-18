from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Callable
from urllib.request import Request, urlopen


def baixar_arquivo(
    *,
    url: str,
    destino: Path,
    sistema_origem: str,
    dataset_origem: str,
    run_id: str,
    abrir: Callable = urlopen,
    tamanho_bloco: int = 1024 * 1024,
    minimo_bytes: int = 1,
    timeout: int = 120,
) -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporario = destino.with_suffix(f"{destino.suffix}.part")
    hash_arquivo = sha256()
    tamanho_bytes = 0
    request = Request(url, headers={"User-Agent": "RastroPublico/0.1"})

    try:
        with abrir(request, timeout=timeout) as resposta, temporario.open("wb") as arquivo:
            while bloco := resposta.read(tamanho_bloco):
                arquivo.write(bloco)
                hash_arquivo.update(bloco)
                tamanho_bytes += len(bloco)
            status_http = resposta.status
            data_publicacao = resposta.headers.get("Last-Modified")
        if tamanho_bytes < minimo_bytes:
            raise ValueError(f"arquivo menor que {minimo_bytes} bytes")
        temporario.replace(destino)
    except Exception:
        temporario.unlink(missing_ok=True)
        raise

    manifesto = destino.with_suffix(f"{destino.suffix}.manifest.json")
    manifesto_temporario = manifesto.with_suffix(f"{manifesto.suffix}.part")
    documento = {
        "run_id": run_id,
        "sistema_origem": sistema_origem,
        "canal_entrega": "repositorio_csv",
        "dataset_origem": dataset_origem,
        "url_origem": url,
        "arquivo": destino.name,
        "coletado_em_utc": datetime.now(UTC).isoformat(),
        "data_publicacao_arquivo": data_publicacao,
        "status_http": status_http,
        "tamanho_bytes": tamanho_bytes,
        "hash_arquivo": hash_arquivo.hexdigest(),
    }
    manifesto_temporario.write_text(
        json.dumps(documento, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    manifesto_temporario.replace(manifesto)
    return manifesto
