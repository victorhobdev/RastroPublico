import json
from hashlib import sha256
from io import BytesIO
from urllib.error import HTTPError

from rastro_publico.coleta.vinculos_pncp import (
    ChaveContrato,
    ControleContratacao,
    extrair_chaves_contrato,
    extrair_controles_statement,
    medir_capacidades_contratuais,
    medir_vinculos,
    preparar_manifestos_bronze,
)


class Resposta:
    def __init__(self, documento: dict, status: int = 200) -> None:
        self.status = status
        self._corpo = BytesIO(json.dumps(documento).encode())

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def read(self) -> bytes:
        return self._corpo.read()


def test_extrai_controles_do_resultado_sql_sem_duplicar() -> None:
    documento = {
        "manifest": {
            "schema": {
                "columns": [
                    {"name": "numero_controle_pncp"},
                    {"name": "itens"},
                ]
            }
        },
        "result": {
            "data_array": [
                ["30051023000196-1-000125/2026", "2"],
                ["30051023000196-1-000125/2026", "1"],
            ]
        },
    }

    assert extrair_controles_statement(documento) == [
        ControleContratacao("30051023000196-1-000125/2026", "30051023000196", 2026, 125)
    ]


def test_mede_ausencia_404_e_pagina_todos_os_vinculos(tmp_path) -> None:
    controles = [
        ControleContratacao("30051023000196-1-000125/2026", "30051023000196", 2026, 125),
        ControleContratacao("00394460000141-1-000001/2021", "00394460000141", 2021, 1),
    ]
    urls = []

    def abrir(request, timeout):
        urls.append(request.full_url)
        if "30051023000196" in request.full_url:
            raise HTTPError(request.full_url, 404, "sem contrato", {}, None)
        pagina = 2 if "pagina=2" in request.full_url else 1
        return Resposta(
            {
                "data": [{"numeroControlePNCP": f"contrato-{pagina}"}],
                "totalPaginas": 2,
            }
        )

    resumo = medir_vinculos(
        controles,
        tmp_path,
        "run-1",
        abrir=abrir,
        dormir=lambda _: None,
        intervalo=0,
    )

    assert resumo["contratacoes_avaliadas"] == 2
    assert resumo["contratacoes_com_vinculo"] == 1
    assert resumo["contratos_encontrados"] == 2
    assert resumo["respostas_404"] == 1
    assert any("pagina=2" in url for url in urls)
    manifesto = json.loads((tmp_path / "manifesto.json").read_text(encoding="utf-8"))
    assert len(manifesto["respostas"]) == 3
    assert len(list(tmp_path.glob("*.json"))) == 3


def test_preserva_422_como_fonte_sem_cadastro(tmp_path) -> None:
    controle = ControleContratacao(
        "04696490000163-1-000064/2026", "04696490000163", 2026, 64
    )
    corpo = b'{"status":"422","message":"Contratacao nao cadastrada"}'

    def abrir(request, timeout):
        raise HTTPError(request.full_url, 422, "nao cadastrada", {}, BytesIO(corpo))

    resumo = medir_vinculos(
        [controle],
        tmp_path,
        "run-422",
        abrir=abrir,
        dormir=lambda _: None,
        intervalo=0,
    )

    assert resumo["respostas_422"] == 1
    assert resumo["contratacoes_com_vinculo"] == 0
    manifesto = json.loads((tmp_path / "manifesto.json").read_text(encoding="utf-8"))
    arquivo = manifesto["respostas"][0]["arquivo"]
    assert (tmp_path / arquivo).read_bytes() == corpo


def test_extrai_contratos_e_mede_detalhe_historico_e_termos(tmp_path) -> None:
    vinculos = tmp_path / "vinculos"
    vinculos.mkdir()
    (vinculos / "vinculo-00001-pagina-001.json").write_text(
        json.dumps(
            {
                "data": [
                    {
                        "numeroControlePNCP": "00038166000105-2-000124/2026",
                        "anoContrato": 2026,
                        "sequencialContrato": 124,
                        "orgaoEntidade": {"cnpj": "00038166000105"},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    chaves = extrair_chaves_contrato(vinculos)
    assert chaves == [
        ChaveContrato("00038166000105-2-000124/2026", "00038166000105", 2026, 124)
    ]

    def abrir(request, timeout):
        url = request.full_url
        if "/historico" in url:
            return Resposta([{"tipo": "inclusao"}, {"tipo": "retificacao"}]) if "pagina=1" in url else Resposta({}, 204)
        if "/termos" in url:
            return Resposta([{"numeroTermoContrato": "1"}]) if "pagina=1" in url else Resposta({}, 204)
        return Resposta({"numeroControlePNCP": chaves[0].numero_controle_pncp})

    resumo = medir_capacidades_contratuais(
        chaves,
        tmp_path / "capacidades",
        "run-capacidades",
        abrir=abrir,
        dormir=lambda _: None,
        intervalo=0,
    )

    assert resumo["contratos_avaliados"] == 1
    assert resumo["detalhes_disponiveis"] == 1
    assert resumo["contratos_com_historico"] == 1
    assert resumo["eventos_historicos"] == 2
    assert resumo["contratos_com_termos"] == 1
    assert resumo["termos_encontrados"] == 1


def test_prepara_manifestos_separa_payload_valido_de_operacao(tmp_path) -> None:
    payload = b'{"data":[{"contrato":"1"}]}'
    (tmp_path / "pagina.json").write_bytes(payload)
    manifesto = {
        "run_id": "run-bronze",
        "respostas": [
            {
                "numero_controle_pncp": "00394460000141-1-000001/2021",
                "endpoint": "https://pncp.gov.br/vinculo",
                "url_origem": "https://pncp.gov.br/vinculo?pagina=1",
                "pagina": 1,
                "status_http": 200,
                "tentativas": 1,
                "coletado_em_utc": "2026-07-18T01:00:00+00:00",
                "arquivo": "pagina.json",
                "hash_payload": sha256(payload).hexdigest(),
            },
            {
                "numero_controle_pncp": "04696490000163-1-000064/2026",
                "endpoint": "https://pncp.gov.br/vinculo",
                "url_origem": "https://pncp.gov.br/vinculo?pagina=1",
                "pagina": 1,
                "status_http": 422,
                "tentativas": 1,
                "coletado_em_utc": "2026-07-18T01:00:01+00:00",
                "arquivo": None,
                "hash_payload": None,
            },
        ],
    }
    caminho = tmp_path / "manifesto.json"
    caminho.write_text(json.dumps(manifesto), encoding="utf-8")

    bronze, operacao = preparar_manifestos_bronze([caminho])

    assert len(bronze) == 1
    assert bronze[0]["payload"] == payload
    assert bronze[0]["capacidade"] == "vinculo"
    assert [linha["status_http"] for linha in operacao] == [200, 422]
