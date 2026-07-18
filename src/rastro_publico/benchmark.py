import re


_HINTS = {
    "natural": "",
    "broadcast": "/*+ BROADCAST(d, o, r) */",
    "merge": "/*+ MERGE(i, d, o, r) */",
}
_SAFE_LABEL = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")


def build_benchmark_sql(strategy: str, run_label: str) -> str:
    try:
        hint = _HINTS[strategy]
    except KeyError as exc:
        raise ValueError(f"estrategia invalida: {strategy}") from exc
    if not _SAFE_LABEL.fullmatch(run_label):
        raise ValueError(f"rotulo de execucao invalido: {run_label}")

    select_hint = f" {hint}" if hint else ""
    return f"""/* rastro_publico:block7 strategy={strategy} run={run_label} */
WITH resultados_por_item AS (
  SELECT
    item_id,
    COUNT(DISTINCT resultado_id) AS resultados,
    SUM(COALESCE(valor_total_homologado, 0)) AS valor_homologado
  FROM workspace.silver.resultados_itens
  GROUP BY item_id
),
agregado AS (
  SELECT{select_hint}
    d.orgao_id,
    COALESCE(i.categoria_tecnologia, 'incerto') AS categoria_tecnologia,
    COUNT(*) AS itens,
    SUM(COALESCE(r.resultados, 0)) AS resultados,
    SUM(COALESCE(i.valor_total, 0)) AS valor_estimado,
    SUM(COALESCE(r.valor_homologado, 0)) AS valor_homologado
  FROM workspace.silver.itens_contratacao i
  INNER JOIN workspace.silver.contratacoes_dimensoes d
    ON d.contratacao_id = i.contratacao_id
  INNER JOIN workspace.silver.orgaos o
    ON o.orgao_id = d.orgao_id
  LEFT JOIN resultados_por_item r
    ON r.item_id = i.item_id
  GROUP BY d.orgao_id, COALESCE(i.categoria_tecnologia, 'incerto')
),
ordenado AS (
  SELECT
    orgao_id,
    categoria_tecnologia,
    itens,
    resultados,
    CAST(valor_estimado AS DECIMAL(38, 2)) AS valor_estimado,
    CAST(valor_homologado AS DECIMAL(38, 2)) AS valor_homologado,
    CONCAT_WS(
      '|',
      orgao_id,
      categoria_tecnologia,
      CAST(itens AS STRING),
      CAST(resultados AS STRING),
      CAST(CAST(valor_estimado AS DECIMAL(38, 2)) AS STRING),
      CAST(CAST(valor_homologado AS DECIMAL(38, 2)) AS STRING)
    ) AS linha_canonica
  FROM agregado
  ORDER BY orgao_id, categoria_tecnologia
)
SELECT
  COUNT(*) AS grupos,
  SUM(itens) AS itens,
  SUM(resultados) AS resultados,
  CAST(SUM(valor_estimado) AS DECIMAL(38, 2)) AS valor_estimado,
  CAST(SUM(valor_homologado) AS DECIMAL(38, 2)) AS valor_homologado,
  SHA2(CONCAT_WS('||', SORT_ARRAY(COLLECT_LIST(linha_canonica))), 256)
    AS checksum_resultado
FROM ordenado"""


def build_explain_sql(strategy: str, run_label: str) -> str:
    return f"EXPLAIN FORMATTED\n{build_benchmark_sql(strategy, run_label)}"


def canonical_benchmark_result(rows: list[list[str | None]]) -> tuple[str | None, ...]:
    if len(rows) != 1 or len(rows[0]) != 6:
        raise ValueError("resultado do benchmark deve conter uma linha e seis colunas")
    return tuple(rows[0])
