from pathlib import Path
from zipfile import ZipFile


ROOT = Path(__file__).parents[1]
DOCUMENT = ROOT / "deliverables/RastroPublico-case-study.docx"


def test_case_study_historico_mantem_imagens_acessiveis() -> None:
    assert DOCUMENT.exists()
    with ZipFile(DOCUMENT) as archive:
        document_xml = archive.read("word/document.xml").decode("utf-8")

    for alt_text in ("arquitetura.png", "categorias.png", "benchmark.png"):
        assert alt_text in document_xml
