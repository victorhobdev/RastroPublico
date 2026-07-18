"""Redesenha o dashboard RastroPublico sem alterar os datasets existentes."""

from __future__ import annotations

import argparse
import configparser
import json
import subprocess
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any


DASHBOARD_ID = "01f182507d3519de8cd5931bef2d613f"
PROFILE = "rastro-publico"
COLORS = [
    "#2F80ED",
    "#56CCF2",
    "#27AE60",
    "#F2C94C",
    "#F2994A",
    "#EB5757",
    "#9B51E0",
    "#6FCF97",
    "#4F4F4F",
    "#BDBDBD",
]

def _credentials(profile: str) -> tuple[str, str]:
    config = configparser.ConfigParser()
    config.read(Path.home() / ".databrickscfg")
    host = config[profile]["host"].rstrip("/")
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", profile, "--output", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    return host, json.loads(result.stdout)["access_token"]


def _request(
    host: str,
    token: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    request = urllib.request.Request(
        f"{host}{path}",
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = response.read()
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")
        raise RuntimeError(f"Databricks respondeu HTTP {error.code}: {detail}") from error
    return json.loads(payload) if payload else {}


def _text(name: str, lines: list[str], x: int, y: int, width: int, height: int):
    return {
        "widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def _chart(
    name: str,
    dataset: str,
    fields: list[dict[str, str]],
    *,
    title: str,
    widget_type: str,
    x_field: str,
    x_title: str,
    x_type: str,
    y_field: str,
    y_title: str,
    y_type: str,
    x: int,
    y: int,
    width: int,
    height: int,
):
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset,
                        "fields": fields,
                        "disaggregated": False,
                    },
                }
            ],
            "spec": {
                "version": 3,
                "widgetType": widget_type,
                "encodings": {
                    "x": {
                        "fieldName": x_field,
                        "displayName": x_title,
                        "scale": {"type": x_type},
                        "axis": {"title": x_title},
                    },
                    "y": {
                        "fieldName": y_field,
                        "displayName": y_title,
                        "scale": {"type": y_type},
                        "axis": {"title": y_title},
                    },
                    "label": {"show": False},
                },
                "frame": {"showTitle": True, "title": title},
                "mark": {"colors": COLORS},
            },
        },
        "position": {"x": x, "y": y, "width": width, "height": height},
    }


def _table(
    name: str,
    dataset: str,
    columns: list[tuple[str, str, str | None]],
    *,
    title: str,
    y: int,
):
    fields = [
        {
            "name": label,
            "expression": f"`{field}`",
        }
        for field, label, _ in columns
    ]
    encodings = []
    for field, label, number_format in columns:
        column: dict[str, Any] = {
            "fieldName": label,
            "displayName": label,
            "title": label,
        }
        if number_format:
            column["numberFormat"] = number_format
        encodings.append(column)
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset,
                        "fields": fields,
                        "disaggregated": True,
                    },
                }
            ],
            "spec": {
                "version": 2,
                "widgetType": "table",
                "frame": {"showTitle": True, "title": title},
                "encodings": {"columns": encodings},
                "data": {"queryName": "main_query"},
            },
        },
        "position": {"x": 0, "y": y, "width": 12, "height": 7},
    }


def build_dashboard(current: dict[str, Any]) -> dict[str, Any]:
    datasets = current["datasets"]
    page_executive = {
        "name": "visao_geral",
        "displayName": "Visão executiva",
        "pageType": "PAGE_TYPE_CANVAS",
        "layoutVersion": "GRID_V1",
        "layout": [
            _text(
                "titulo_executivo",
                [
                    "# RastroPúblico — compras de tecnologia\n",
                    "**Panorama de órgãos, fornecedores, itens e contratos públicos.**  \n",
                    "Janela analisada: **18 jul. 2025 a 17 jul. 2026** · Fontes oficiais Compras.gov e Comprasnet.  \n",
                    "Os indicadores descrevem concentração e recorrência; não classificam fraude ou irregularidade.",
                ],
                0,
                0,
                12,
                2,
            ),
            _text(
                "kpi_contratacoes",
                ["## 317.043\n", "**contratações**\n", "registros correntes no recorte"],
                0,
                2,
                3,
                2,
            ),
            _text(
                "kpi_itens",
                ["## 2,63 milhões\n", "**itens tratados**\n", "após deduplicação e validação"],
                3,
                2,
                3,
                2,
            ),
            _text(
                "kpi_fornecedores",
                ["## 106.494\n", "**fornecedores**\n", "identificados na camada analítica"],
                6,
                2,
                3,
                2,
            ),
            _text(
                "kpi_contratos",
                ["## 52.767\n", "**contratos**\n", "com vigência e eventos acompanhados"],
                9,
                2,
                3,
                2,
            ),
            _text(
                "leituras_principais",
                [
                    "## O que este recorte revela\n",
                    "- **11.548 relações recorrentes** conectam órgãos e fornecedores ao longo do período.\n",
                    "- Apenas **1.072 de 8.564 grupos (12,5%)** atingiram cobertura suficiente para publicar concentração.\n",
                    "- Os **93 grupos de preço** foram avaliados, mas nenhum permite comparação defensável nesta versão.",
                ],
                0,
                4,
                12,
                3,
            ),
            _text(
                "limite_classificacao",
                [
                    "## A classificação ainda é o principal limite\n",
                    "A categoria **não identificada concentra a maior parte dos itens** do recorte.\n",
                    "\nPor isso, apenas **12,5% dos grupos de concentração** atingiram cobertura suficiente para publicação. A limitação é exibida para evitar conclusões com falsa precisão.",
                ],
                0,
                7,
                6,
                7,
            ),
            _chart(
                "evolucao_mensal",
                "6c584383",
                [
                    {"name": "periodo", "expression": "`periodo`"},
                    {"name": "itens", "expression": "SUM(`total_itens`)"},
                ],
                title="Como o volume evoluiu mês a mês?",
                widget_type="line",
                x_field="periodo",
                x_title="Período",
                x_type="categorical",
                y_field="itens",
                y_title="Itens analisados",
                y_type="quantitative",
                x=6,
                y=7,
                width=6,
                height=7,
            ),
            _text(
                "qualidade_interpretacao",
                [
                    "## Qualidade antes da interpretação\n",
                    "- **21,16%** de repetições técnicas controladas\n",
                    "- **4,99%** de conflitos de versão preservados\n",
                    "- **1,40%** de vínculos pendentes\n",
                    "- **0,03%** de registros em quarentena\n",
                    "\nEsses percentuais explicam diferenças entre camadas; não são acusações sobre compras.",
                ],
                0,
                14,
                4,
                6,
            ),
            _text(
                "leitura_concentracao",
                [
                    "## Como interpretar concentração\n",
                    "O **HHI** resume quanto a participação está concentrada em poucos fornecedores: valores maiores indicam maior concentração.\n",
                    "\nO índice só é publicado quando identificação do fornecedor e valor possuem cobertura defensável. **Concentração é um sinal descritivo, não uma prova de irregularidade.**",
                ],
                4,
                14,
                8,
                6,
            ),
        ],
    }

    page_relations = {
        "name": "211c9bb3",
        "displayName": "Relações e contratos",
        "pageType": "PAGE_TYPE_CANVAS",
        "layoutVersion": "GRID_V1",
        "layout": [
            _text(
                "titulo_relacoes",
                [
                    "# Relações que merecem investigação\n",
                    "A página prioriza **recorrência, alcance do fornecedor e evolução contratual**.  \n",
                    "Comece pelos primeiros registros: as fontes já estão ordenadas por frequência, presença ou quantidade de eventos.",
                ],
                0,
                0,
                12,
                2,
            ),
            _text(
                "kpi_relacoes",
                ["## 11.548\n", "**relações recorrentes**\n", "entre órgãos e fornecedores"],
                0,
                2,
                4,
                2,
            ),
            _text(
                "kpi_presenca",
                ["## 5.790\n", "**fornecedores com presença**\n", "medida por órgãos, UFs e períodos"],
                4,
                2,
                4,
                2,
            ),
            _text(
                "kpi_eventos",
                ["## 60.324\n", "**eventos contratuais**\n", "usados para reconstruir a evolução"],
                8,
                2,
                4,
                2,
            ),
            _text(
                "como_ler_tabelas",
                [
                    "### Como ler\n",
                    "Mais contratos e mais períodos indicam **recorrência**; mais órgãos e UFs indicam **presença pública**; mais eventos e dias de extensão indicam **maior evolução contratual**. Nenhum desses sinais comprova irregularidade.",
                ],
                0,
                4,
                12,
                2,
            ),
            _table(
                "relacoes_prioritarias",
                "recorrencia",
                [
                    ("orgao", "Órgão", None),
                    ("fornecedor", "Fornecedor", None),
                    ("contratacoes_distintas", "Contratações", "0"),
                    ("periodos_distintos", "Períodos", "0"),
                    ("valor_total_homologado", "Valor homologado (R$)", "0.00"),
                ],
                title="Relações órgão–fornecedor mais recorrentes",
                y=6,
            ),
            _table(
                "fornecedores_presenca",
                "presenca_contexto",
                [
                    ("fornecedor", "Fornecedor", None),
                    ("orgaos_distintos", "Órgãos", "0"),
                    ("ufs_distintas", "UFs", "0"),
                    ("municipios_distintos", "Municípios", "0"),
                    ("contratacoes_distintas", "Contratações", "0"),
                    ("valor_total_homologado", "Valor homologado (R$)", "0.00"),
                    ("registros_ceis", "Registros CEIS", "0"),
                    ("registros_cnep", "Registros CNEP", "0"),
                ],
                title="Fornecedores com maior presença pública",
                y=13,
            ),
            _table(
                "contratos_evolucao",
                "evolucao",
                [
                    ("orgao_codigo", "Código do órgão", None),
                    ("vigencia_inicio", "Início da vigência", None),
                    ("vigencia_fim_atual", "Fim da vigência atual", None),
                    ("valor_inicial", "Valor inicial (R$)", "0.00"),
                    ("valor_global", "Valor global (R$)", "0.00"),
                    ("eventos_distintos", "Eventos", "0"),
                    ("variacao_valor_acumulada", "Variação acumulada (R$)", "0.00"),
                    ("extensao_vigencia_dias", "Extensão (dias)", "0"),
                ],
                title="Contratos com mais alterações registradas",
                y=20,
            ),
            _text(
                "precos_limitacao",
                [
                    "## Comparação de preços: resultado não publicável\n",
                    "Os **93 grupos avaliados** não apresentam unidade, categoria e escopo suficientemente comparáveis.  \n",
                    "Por isso, os percentis calculados não aparecem como ranking e **não devem ser interpretados como sobrepreço**. A limitação é parte do resultado analítico.",
                ],
                0,
                27,
                12,
                3,
            ),
        ],
    }
    return {"datasets": datasets, "pages": [page_executive, page_relations]}


def validate_dashboard(dashboard: dict[str, Any]) -> None:
    datasets = {dataset["name"] for dataset in dashboard["datasets"]}
    assert len(dashboard["pages"]) == 2
    names: set[str] = set()
    for page in dashboard["pages"]:
        occupied: list[tuple[int, int, int, int]] = []
        for item in page["layout"]:
            widget = item["widget"]
            assert widget["name"] not in names, f"widget duplicado: {widget['name']}"
            names.add(widget["name"])
            for query in widget.get("queries", []):
                dataset = query["query"]["datasetName"]
                assert dataset in datasets, f"dataset ausente: {dataset}"
            position = item["position"]
            box = (
                position["x"],
                position["y"],
                position["x"] + position["width"],
                position["y"] + position["height"],
            )
            for other in occupied:
                overlap = box[0] < other[2] and box[2] > other[0] and box[1] < other[3] and box[3] > other[1]
                assert not overlap, f"widgets sobrepostos em {page['displayName']}"
            occupied.append(box)
    visible_text = []
    for page in dashboard["pages"]:
        visible_text.append(page["displayName"])
        for item in page["layout"]:
            widget = item["widget"]
            visible_text.extend(widget.get("multilineTextboxSpec", {}).get("lines", []))
            spec = widget.get("spec", {})
            visible_text.append(spec.get("frame", {}).get("title", ""))
            visible_text.extend(
                column.get("title", "")
                for column in spec.get("encodings", {}).get("columns", [])
            )
    serialized = " ".join(visible_text)
    for error in (
        "RastroPublico",
        "Visao",
        "Analises",
        "orgao-fornecedor",
        "nao publicavel",
        "_",
    ):
        assert error not in serialized, f"texto sem revisão: {error}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=PROFILE)
    parser.add_argument("--dashboard-id", default=DASHBOARD_ID)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    host, token = _credentials(args.profile)
    path = f"/api/2.0/lakeview/dashboards/{args.dashboard_id}"
    current_asset = _request(host, token, "GET", path)
    current = json.loads(current_asset["serialized_dashboard"])
    redesigned = build_dashboard(current)
    validate_dashboard(redesigned)
    widgets = sum(len(page["layout"]) for page in redesigned["pages"])
    print(f"Definição válida: 2 páginas, {widgets} widgets, {len(redesigned['datasets'])} datasets.")
    if not args.apply:
        return

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = _request(
        host,
        token,
        "POST",
        "/api/2.0/lakeview/dashboards",
        {
            "display_name": f"RastroPublico - Backup antes do redesign {timestamp}",
            "warehouse_id": current_asset["warehouse_id"],
            "serialized_dashboard": current_asset["serialized_dashboard"],
            "parent_path": current_asset["parent_path"],
        },
    )
    print(f"Backup criado: {backup['dashboard_id']}")

    updated = _request(
        host,
        token,
        "PATCH",
        path,
        {
            "display_name": "RastroPúblico — Panorama de compras de tecnologia",
            "warehouse_id": current_asset["warehouse_id"],
            "serialized_dashboard": json.dumps(redesigned, ensure_ascii=False),
            "etag": current_asset["etag"],
        },
    )
    _request(
        host,
        token,
        "POST",
        f"{path}/published",
        {"embed_credentials": True},
    )
    print(f"Dashboard atualizado e publicado: {updated['dashboard_id']}")


if __name__ == "__main__":
    main()
