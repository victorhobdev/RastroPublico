"""Publica uma apresentação auditada do RastroPúblico no AI/BI Dashboard."""

from __future__ import annotations

import argparse
import configparser
import json
import subprocess
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any


DASHBOARD_ID = "01f182507d3519de8cd5931bef2d613f"


def _credentials(profile: str) -> tuple[str, str]:
    config = configparser.ConfigParser()
    config.read(Path.home() / ".databrickscfg")
    token = subprocess.run(
        ["databricks", "auth", "token", "--profile", profile, "--output", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    return config[profile]["host"].rstrip("/"), json.loads(token.stdout)["access_token"]


def _request(
    host: str, token: str, method: str, path: str, body: dict[str, Any] | None = None
) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{host}{path}",
        data=(json.dumps(body, ensure_ascii=False).encode() if body else None),
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = response.read()
    return json.loads(payload) if payload else {}


def _text(name: str, lines: list[str], x: int, y: int, width: int, height: int):
    return {
        "widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def build_dashboard() -> dict[str, Any]:
    datasets: list[dict[str, Any]] = []
    executive = {
        "name": "visao_executiva",
        "displayName": "Visão executiva",
        "pageType": "PAGE_TYPE_CANVAS",
        "layoutVersion": "GRID_V1",
        "layout": [
            _text(
                "titulo",
                [
                    "# RastroPúblico — compras públicas de tecnologia\n",
                    "**Quem compra, quem fornece e quais relações se repetem.**  \n",
                    "Brasil · **18 jul. 2025 a 17 jul. 2026** · Compras.gov e Comprasnet · Sem classificação de fraude ou irregularidade.",
                ],
                0,
                0,
                12,
                2,
            ),
            _text(
                "compras",
                ["## 12.252\n", "**compras de tecnologia**\n", "publicadas na janela"],
                0,
                2,
                3,
                2,
            ),
            _text(
                "itens",
                ["## 29.207\n", "**itens classificados**\n", "em 11 categorias"],
                3,
                2,
                3,
                2,
            ),
            _text(
                "fornecedores",
                [
                    "## 4.246\n",
                    "**fornecedores distintos**\n",
                    "em compras ou contratos",
                ],
                6,
                2,
                3,
                2,
            ),
            _text(
                "contratos",
                [
                    "## 1.537\n",
                    "**contratos de tecnologia**\n",
                    "identificados pelos itens",
                ],
                9,
                2,
                3,
                2,
            ),
            _text(
                "leitura",
                [
                    "## A leitura em 30 segundos\n",
                    "- **828 relações** tiveram duas ou mais compras distintas.\n",
                    "- **Licenciamento** é a maior categoria: 7.445 itens (25,5%).\n",
                    "- Os contratos tecnológicos registram **1.755 eventos** na janela.\n",
                    "- Totais monetários e rankings de preço foram **suprimidos** pelo gate de qualidade.",
                ],
                0,
                4,
                12,
                3,
            ),
            _text(
                "categorias_itens",
                [
                    "## Distribuição dos itens classificados\n",
                    "- **Licenciamento:** 7.445 · **Computadores e notebooks:** 4.607\n",
                    "- **Impressoras e scanners:** 3.167 · **Redes:** 2.739\n",
                    "- **Outsourcing:** 2.649 · **Cloud:** 2.607\n",
                    "- **Infraestrutura:** 1.872 · **Monitores:** 1.800\n",
                    "- **Suporte:** 1.243 · **Servidores:** 737\n",
                    "- **Desenvolvimento:** 341",
                ],
                0,
                7,
                8,
                6,
            ),
            _text(
                "recorrencia",
                [
                    "## O que significa recorrência?\n",
                    "A mesma combinação **órgão + fornecedor + categoria** aparece em pelo menos duas compras distintas.\n",
                    "\nPode refletir especialização, contratos sucessivos ou demanda continuada.",
                ],
                8,
                7,
                4,
                3,
            ),
            _text(
                "precos",
                [
                    "## Por que não há ranking de preços?\n",
                    "Descrição, unidade e escopo não asseguram equivalência. Outliers extremos também tornam somas brutas indefensáveis.\n",
                    "\nA ausência do ranking é um resultado de qualidade.",
                ],
                8,
                10,
                4,
                4,
            ),
        ],
    }
    evidence = {
        "name": "evidencias_limites",
        "displayName": "Evidências e limites",
        "pageType": "PAGE_TYPE_CANVAS",
        "layoutVersion": "GRID_V1",
        "layout": [
            _text(
                "titulo_evidencias",
                [
                    "# Da fonte à evidência\n",
                    "O produto foi recalibrado após auditoria semântica. Esta página separa o que os dados comprovam do que não permitem concluir.",
                ],
                0,
                0,
                12,
                2,
            ),
            _text(
                "relacoes",
                ["## 828\n", "**relações recorrentes**\n", "duas ou mais compras"],
                0,
                2,
                3,
                2,
            ),
            _text(
                "resultados",
                ["## 20.664\n", "**resultados de itens**\n", "ligados à tecnologia"],
                3,
                2,
                3,
                2,
            ),
            _text(
                "eventos",
                ["## 1.755\n", "**eventos contratuais**\n", "no recorte tecnológico"],
                6,
                2,
                3,
                2,
            ),
            _text(
                "spark",
                [
                    "## 4,79 milhões\n",
                    "**linhas no benchmark**\n",
                    "39 arquivos · zero spill",
                ],
                9,
                2,
                3,
                2,
            ),
            _text(
                "pipeline",
                [
                    "## Pipeline auditável\n",
                    "**Landing imutável + manifests** → staging → Silver versionada → Gold com gates.\n",
                    "\nA ingestão é incremental. O núcleo Silver é reconstruído integralmente na escala atual; essa limitação é explícita.",
                ],
                0,
                4,
                6,
                5,
            ),
            _text(
                "benchmark",
                [
                    "## Experimento Spark no mesmo ambiente\n",
                    "- natural/AQE: **3,19 s** de mediana\n",
                    "- broadcast explícito: **3,23 s**\n",
                    "- sort-merge forçado: **6,95 s**\n",
                    "\nDecisão: manter o plano natural. O resultado vale para esta consulta e este compute.",
                ],
                6,
                4,
                6,
                5,
            ),
            _text(
                "limites",
                [
                    "## Limites visíveis\n",
                    "- Classificação determinística por descrição.\n",
                    "- Vínculo PNCP–contrato incompleto na fonte.\n",
                    "- Preços sem especificação suficiente para comparação.\n",
                    "- CEIS/CNEP são contexto oficial, não conclusão sobre uma compra.",
                ],
                0,
                9,
                6,
                6,
            ),
            _text(
                "exports",
                [
                    "## Evidências no repositório\n",
                    "- Jobs reproduzíveis em bundle;\n",
                    "- saída integral do benchmark;\n",
                    "- métricas brutas do Query History;\n",
                    "- especificação deste dashboard;\n",
                    "- KPIs recalculados dos snapshots anuais.",
                ],
                6,
                9,
                6,
                6,
            ),
        ],
    }
    return {"datasets": datasets, "pages": [executive, evidence]}


def validate_dashboard(dashboard: dict[str, Any]) -> None:
    datasets = {dataset["name"] for dataset in dashboard["datasets"]}
    names: set[str] = set()
    for page in dashboard["pages"]:
        occupied: list[tuple[int, int, int, int]] = []
        for item in page["layout"]:
            widget = item["widget"]
            assert widget["name"] not in names
            names.add(widget["name"])
            for query in widget.get("queries", []):
                assert query["query"]["datasetName"] in datasets
            position = item["position"]
            box = (
                position["x"],
                position["y"],
                position["x"] + position["width"],
                position["y"] + position["height"],
            )
            assert not any(
                box[0] < other[2]
                and box[2] > other[0]
                and box[1] < other[3]
                and box[3] > other[1]
                for other in occupied
            )
            occupied.append(box)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="rastro-publico")
    parser.add_argument("--dashboard-id", default=DASHBOARD_ID)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    dashboard = build_dashboard()
    validate_dashboard(dashboard)
    widget_count = sum(len(page["layout"]) for page in dashboard["pages"])
    print(
        f"Definição válida: 2 páginas, {widget_count} widgets, sem consulta ao compute."
    )
    if not args.apply:
        return

    host, token = _credentials(args.profile)
    path = f"/api/2.0/lakeview/dashboards/{args.dashboard_id}"
    current = _request(host, token, "GET", path)
    backup = _request(
        host,
        token,
        "POST",
        "/api/2.0/lakeview/dashboards",
        {
            "display_name": "RastroPúblico - backup "
            + datetime.now().strftime("%Y%m%d-%H%M%S"),
            "warehouse_id": current["warehouse_id"],
            "serialized_dashboard": current["serialized_dashboard"],
            "parent_path": current["parent_path"],
        },
    )
    print(f"Backup criado: {backup['dashboard_id']}")
    updated = _request(
        host,
        token,
        "PATCH",
        path,
        {
            "display_name": "RastroPúblico — compras públicas de tecnologia",
            "warehouse_id": current["warehouse_id"],
            "serialized_dashboard": json.dumps(dashboard, ensure_ascii=False),
            "etag": current["etag"],
        },
    )
    _request(host, token, "POST", f"{path}/published", {"embed_credentials": True})
    print(f"Dashboard atualizado e publicado: {updated['dashboard_id']}")


if __name__ == "__main__":
    main()
