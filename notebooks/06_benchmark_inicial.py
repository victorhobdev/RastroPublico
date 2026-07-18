# Databricks notebook source
# ruff: noqa: E402, F821
import json
import sys
from time import perf_counter


dbutils.widgets.text("source_root", "/Workspace/Users/victorhob@ufrrj.br/rastro_publico/src")
dbutils.widgets.dropdown(
    "strategy", "all", ["all", "natural", "broadcast", "merge"]
)
dbutils.widgets.text("run_label", "iteracao-01")
dbutils.widgets.text("repeticoes", "4")

source_root = dbutils.widgets.get("source_root")
if source_root not in sys.path:
    sys.path.insert(0, source_root)

from rastro_publico.benchmark import (  # noqa: E402
    build_benchmark_sql,
    build_explain_sql,
    canonical_benchmark_result,
    canonical_plan,
)


strategy = dbutils.widgets.get("strategy")
run_label = dbutils.widgets.get("run_label")
repeticoes = int(dbutils.widgets.get("repeticoes"))
if repeticoes != 4:
    raise ValueError("benchmark inicial exige um aquecimento e tres medidas")

# Evita que o cache de resultado transforme repetições em leituras do resultado
# anterior. O cache de I/O do engine permanece parte do ambiente medido.
spark.sql("SET use_cached_result = false")

strategies = ["natural", "broadcast", "merge"] if strategy == "all" else [strategy]
initial_plans = {}
for current_strategy in strategies:
    initial_plan = spark.sql(
        build_explain_sql(current_strategy, f"plano-{run_label}-{current_strategy}")
    )
    initial_plans[current_strategy] = canonical_plan(initial_plan.collect())
    print(f"PLANO_INICIAL strategy={current_strategy}")
    print(initial_plans[current_strategy])

sequence = [(current_strategy, 0, True) for current_strategy in strategies]
if strategy == "all":
    sequence += [
        ("natural", 1, False),
        ("broadcast", 1, False),
        ("merge", 1, False),
        ("broadcast", 2, False),
        ("merge", 2, False),
        ("natural", 2, False),
        ("merge", 3, False),
        ("natural", 3, False),
        ("broadcast", 3, False),
    ]
else:
    sequence += [(strategy, iteration, False) for iteration in range(1, 4)]

executions = []
for current_strategy, iteration, warmup in sequence:
    iteration_label = f"{run_label}-{current_strategy}-{iteration:02d}"
    started = perf_counter()
    result_rows = [
        list(row)
        for row in spark.sql(
            build_benchmark_sql(current_strategy, iteration_label)
        ).collect()
    ]
    elapsed_ms = round(1000 * (perf_counter() - started), 3)
    executions.append(
        {
            "strategy": current_strategy,
            "iteration": iteration,
            "warmup": warmup,
            "elapsed_ms": elapsed_ms,
            "result": canonical_benchmark_result(result_rows),
        }
    )

canonical_result = executions[0]["result"]
if any(execution["result"] != canonical_result for execution in executions[1:]):
    raise RuntimeError("resultado logico variou entre repeticoes")
output = {
    "strategy": strategy,
    "run_label": run_label,
    "result": canonical_result,
    "initial_plans": initial_plans,
    "executions": executions,
}
print(json.dumps(output, ensure_ascii=False))
dbutils.notebook.exit(json.dumps(output, ensure_ascii=False))
