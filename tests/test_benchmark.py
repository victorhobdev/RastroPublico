from decimal import Decimal
from pathlib import Path

import pytest

from rastro_publico.benchmark import (
    build_benchmark_sql,
    build_explain_sql,
    canonical_benchmark_result,
    canonical_plan,
)


@pytest.mark.parametrize(
    ("strategy", "expected_hint"),
    [
        ("natural", ""),
        ("broadcast", "BROADCAST(d, o, r)"),
        ("merge", "MERGE(i, d, o, r)"),
    ],
)
def test_build_benchmark_sql_uses_only_the_requested_strategy(
    strategy: str, expected_hint: str
) -> None:
    sql = build_benchmark_sql(strategy, "iteracao-01")

    assert f"strategy={strategy}" in sql
    assert "run=iteracao-01" in sql
    assert expected_hint in sql
    assert "ORDER BY orgao_id, categoria_tecnologia" in sql


def test_benchmark_disables_result_cache_before_measurements() -> None:
    notebook = Path("notebooks/06_benchmark_inicial.py").read_text(encoding="utf-8")

    assert 'spark.sql("SET use_cached_result = false")' in notebook


def test_build_benchmark_sql_rejects_untrusted_labels() -> None:
    with pytest.raises(ValueError, match="rotulo"):
        build_benchmark_sql("natural", "x */ DROP TABLE gold.foo")


def test_build_benchmark_sql_rejects_unknown_strategy() -> None:
    with pytest.raises(ValueError, match="estrategia"):
        build_benchmark_sql("cartesian", "iteracao-01")


def test_explain_wraps_the_same_query() -> None:
    query = build_benchmark_sql("broadcast", "plano-inicial")

    assert build_explain_sql("broadcast", "plano-inicial") == (
        f"EXPLAIN FORMATTED\n{query}"
    )


def test_canonical_plan_preserves_explain_output() -> None:
    assert canonical_plan([["Physical Plan"], ["BroadcastHashJoin"]]) == (
        "Physical Plan\nBroadcastHashJoin"
    )


def test_canonical_result_accepts_the_single_summary_row() -> None:
    row = ["534", "15207", "1021", "123.45", "99.90", "abc123"]

    assert canonical_benchmark_result([row]) == tuple(row)


def test_canonical_result_serializes_spark_decimal_values() -> None:
    row = [527, 15207, 1021, Decimal("123.45"), Decimal("99.90"), "abc123"]

    assert canonical_benchmark_result([row]) == (
        "527",
        "15207",
        "1021",
        "123.45",
        "99.90",
        "abc123",
    )


@pytest.mark.parametrize("rows", [[], [["incompleta"]], [["a"] * 6, ["b"] * 6]])
def test_canonical_result_rejects_incomplete_or_ambiguous_output(
    rows: list[list[str]],
) -> None:
    with pytest.raises(ValueError, match="resultado"):
        canonical_benchmark_result(rows)
