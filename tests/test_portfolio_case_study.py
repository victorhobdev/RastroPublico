import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / "scripts/build_portfolio_case_study.py"


def test_gerador_recusa_evidencias_ausentes() -> None:
    resultado = subprocess.run(
        [sys.executable, str(SCRIPT)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert resultado.returncode != 0
    assert "evidencia ausente" in resultado.stderr
