# Databricks notebook source
# ruff: noqa: E402, F821
import json
import sys
from time import perf_counter


dbutils.widgets.text("source_root", "/Workspace/Users/victorhob@ufrrj.br/rastro_publico/src")
dbutils.widgets.dropdown("strategy", "natural", ["natural", "broadcast", "merge"])
dbutils.widgets.text("run_label", "iteracao-01")
dbutils.widgets.text("repeticoes", "4")

source_root = dbutils.widgets.get("source_root")
if source_root not in sys.path:
    sys.path.insert(0, source_root)

from rastro_publico.benchmark import (  # noqa: E402
    build_benchmark_sql,
    build_explain_sql,
    canonical_benchmark_result,
)


strategy = dbutils.widgets.get("strategy")
run_label = dbutils.widgets.get("run_label")
repeticoes = int(dbutils.widgets.get("repeticoes"))
if repeticoes < 2:
    raise ValueError("repeticoes deve incluir aquecimento e ao menos uma medida")

initial_plan = spark.sql(build_explain_sql(strategy, f"plano-{run_label}"))
print("PLANO_INICIAL")
print("\n".join(row[0] for row in initial_plan.collect()))

spark.sql("SET use_cached_result = false").collect()
executions = []
for iteration in range(1, repeticoes + 1):
    iteration_label = f"{run_label}-{iteration:02d}"
    started = perf_counter()
    result_rows = [
        list(row)
        for row in spark.sql(build_benchmark_sql(strategy, iteration_label)).collect()
    ]
    elapsed_ms = round(1000 * (perf_counter() - started), 3)
    executions.append(
        {
            "iteration": iteration,
            "warmup": iteration == 1,
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
    "executions": executions,
}
print(json.dumps(output, ensure_ascii=False))
dbutils.notebook.exit(json.dumps(output, ensure_ascii=False))
