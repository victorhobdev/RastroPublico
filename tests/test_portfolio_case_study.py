from pathlib import Path
from zipfile import ZipFile


ROOT = Path(__file__).parents[1]
DOCUMENT = ROOT / "deliverables/RastroPublico-case-study.docx"


def test_case_study_usa_baseline_corrigida_e_imagens_acessiveis() -> None:
    assert DOCUMENT.exists()
    with ZipFile(DOCUMENT) as archive:
        document_xml = archive.read("word/document.xml").decode("utf-8")

    for expected in ("12.252", "29.207", "4.246", "1.537", "828"):
        assert expected in document_xml
    assert "113 testes" in document_xml
    for invalidated in ("317.043", "106.494", "52.767", "11.548"):
        assert invalidated not in document_xml
    for alt_text in ("arquitetura.png", "categorias.png", "benchmark.png"):
        assert alt_text in document_xml
