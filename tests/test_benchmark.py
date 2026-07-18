import pytest

from rastro_publico.benchmark import (
    build_benchmark_sql,
    build_explain_sql,
    canonical_benchmark_result,
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


def test_canonical_result_accepts_the_single_summary_row() -> None:
    row = ["534", "15207", "1021", "123.45", "99.90", "abc123"]

    assert canonical_benchmark_result([row]) == tuple(row)


@pytest.mark.parametrize("rows", [[], [["incompleta"]], [["a"] * 6, ["b"] * 6]])
def test_canonical_result_rejects_incomplete_or_ambiguous_output(
    rows: list[list[str]],
) -> None:
    with pytest.raises(ValueError, match="resultado"):
        canonical_benchmark_result(rows)
