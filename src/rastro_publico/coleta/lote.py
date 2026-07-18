from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path


@dataclass(frozen=True)
class LotePreparado:
    bronze: list[dict]
    runs: list[dict]
    requests: list[dict]


def preparar_lote(caminho_manifesto: Path) -> LotePreparado:
    documento = json.loads(caminho_manifesto.read_text(encoding="utf-8"))
    run_id = documento["run_id"]
    respostas = documento["respostas"]
    if not respostas:
        raise ValueError("manifesto sem respostas")

    bronze: list[dict] = []
    requests: list[dict] = []
    for resposta in respostas:
        arquivo = resposta["arquivo"]
        if arquivo is not None:
            payload = _ler_payload(caminho_manifesto.parent, arquivo)
            hash_observado = sha256(payload).hexdigest()
            if hash_observado != resposta["hash_payload"]:
                raise ValueError(f"hash divergente: {arquivo}")
            chave = (
                f"{run_id}|{resposta['endpoint']}|"
                f"{resposta['data_inicio_consulta']}|{resposta['data_fim_consulta']}|"
                f"{resposta['modalidade']}|{resposta['pagina']}"
            )
            bronze.append(
                {
                    "observacao_id": sha256(chave.encode()).hexdigest(),
                    "run_id": run_id,
                    "endpoint": resposta["endpoint"],
                    "url_origem": resposta["url_origem"],
                    "data_inicio_consulta": resposta["data_inicio_consulta"],
                    "data_fim_consulta": resposta["data_fim_consulta"],
                    "modalidade": resposta["modalidade"],
                    "pagina": resposta["pagina"],
                    "coletado_em_utc": resposta["coletado_em_utc"],
                    "hash_payload": hash_observado,
                    "payload": payload,
                }
            )

        for tentativa in resposta["tentativas"]:
            chave = (
                f"{run_id}|{resposta['endpoint']}|"
                f"{resposta['data_inicio_consulta']}|{resposta['data_fim_consulta']}|"
                f"{resposta['modalidade']}|{resposta['pagina']}|"
                f"{tentativa['numero']}"
            )
            requests.append(
                {
                    "request_id": sha256(chave.encode()).hexdigest(),
                    "run_id": run_id,
                    "endpoint": resposta["endpoint"],
                    "modalidade": resposta["modalidade"],
                    "pagina": resposta["pagina"],
                    "tentativa": tentativa["numero"],
                    "status_http": tentativa["status_http"],
                    "duracao_ms": tentativa["duracao_ms"],
                    "erro": tentativa["erro"],
                }
            )

    sucesso = all(resposta["status_http"] in {200, 204} for resposta in respostas)
    runs = [
        {
            "run_id": run_id,
            "criado_em_utc": documento["criado_em_utc"],
            "status": "SUCESSO" if sucesso else "FALHA",
            "total_respostas": len(respostas),
            "respostas_com_payload": len(bronze),
            "total_tentativas": len(requests),
        }
    ]
    return LotePreparado(bronze=bronze, runs=runs, requests=requests)


def _ler_payload(diretorio: Path, arquivo: str) -> bytes:
    caminho = (diretorio / arquivo).resolve()
    if Path(arquivo).name != arquivo or caminho.parent != diretorio.resolve():
        raise ValueError(f"arquivo invalido: {arquivo}")
    return caminho.read_bytes()
