import csv
import zipfile

from rastro_publico.coleta.contexto import (
    filtrar_cnpj_fornecedores,
    filtrar_sancoes_fornecedores,
)


def _zip_csv(caminho, nome, linhas):
    with zipfile.ZipFile(caminho, "w") as arquivo:
        arquivo.writestr(nome, "\n".join(";".join(linha) for linha in linhas))


def test_filtra_empresas_e_minimiza_socios(tmp_path) -> None:
    _zip_csv(
        tmp_path / "Empresas0.zip",
        "EMPRESAS.CSV",
        [
            ["12345678", "Fornecedor alvo", "2062", "49", "1000,00", "03", ""],
            ["99999999", "Outra empresa", "2062", "49", "50,00", "01", ""],
        ],
    )
    _zip_csv(
        tmp_path / "Socios0.zip",
        "SOCIOS.CSV",
        [
            [
                "12345678",
                "2",
                "NOME PESSOAL",
                "***123456**",
                "49",
                "20200101",
                "",
                "***000000**",
                "REPRESENTANTE",
                "05",
                "4",
            ],
            ["99999999", "2", "OUTRO", "***", "49", "20200101", "", "", "", "", "4"],
        ],
    )
    empresas = tmp_path / "saida" / "empresas.csv"
    socios = tmp_path / "saida" / "socios.csv"

    resumo = filtrar_cnpj_fornecedores(
        diretorio=tmp_path,
        cnpjs={"12345678000199"},
        destino_empresas=empresas,
        destino_socios=socios,
    )

    with empresas.open(encoding="utf-8", newline="") as stream:
        linhas_empresas = list(csv.DictReader(stream))
    with socios.open(encoding="utf-8", newline="") as stream:
        linhas_socios = list(csv.DictReader(stream))

    assert resumo == {"cnpjs_basicos_alvo": 1, "empresas": 1, "socios": 1}
    assert linhas_empresas[0]["razao_social"] == "Fornecedor alvo"
    assert linhas_socios[0] == {
        "registro_origem_id": "1",
        "cnpj_basico": "12345678",
        "tipo_socio": "2",
        "qualificacao_socio": "49",
        "data_entrada_sociedade": "20200101",
        "pais": "",
        "qualificacao_representante": "05",
        "faixa_etaria": "4",
    }
    assert "NOME PESSOAL" not in socios.read_text(encoding="utf-8")
    assert "***" not in socios.read_text(encoding="utf-8")


def test_rejeita_identificador_que_nao_e_cnpj(tmp_path) -> None:
    _zip_csv(tmp_path / "Empresas0.zip", "EMPRESAS.CSV", [])
    _zip_csv(tmp_path / "Socios0.zip", "SOCIOS.CSV", [])

    resumo = filtrar_cnpj_fornecedores(
        diretorio=tmp_path,
        cnpjs={"12345678901", "", "invalido"},
        destino_empresas=tmp_path / "empresas.csv",
        destino_socios=tmp_path / "socios.csv",
    )

    assert resumo["cnpjs_basicos_alvo"] == 0


def test_filtra_sancoes_sem_expor_nomes(tmp_path) -> None:
    ceis = tmp_path / "ceis.zip"
    _zip_csv(
        ceis,
        "CEIS.csv",
        [
            [f"campo{i}" for i in range(24)],
            [
                "CEIS",
                "99",
                "J",
                "12345678000199",
                "NOME SANCIONADO",
                "NOME INFORMADO",
                "RAZAO",
                "FANTASIA",
                "processo",
                "categoria",
                "01/01/2026",
                "31/12/2026",
                "02/01/2026",
                "publicacao",
                "detalhe",
                "",
                "Nacional",
                "Orgao",
                "RJ",
                "FEDERAL",
                "fundamento",
                "03/01/2026",
                "origem",
                "obs",
            ],
        ],
    )
    destino = tmp_path / "sancoes.csv"

    contagem = filtrar_sancoes_fornecedores(
        arquivo=ceis,
        cadastro="CEIS",
        cnpjs={"12345678000199"},
        destino=destino,
    )

    texto = destino.read_text(encoding="utf-8")
    assert contagem == 1
    assert "12345678000199" in texto
    assert "NOME SANCIONADO" not in texto
    assert "NOME INFORMADO" not in texto
