# Databricks notebook source
# ruff: noqa: E402, F821
import json
import sys


dbutils.widgets.text("source_root", "/Workspace/Users/victorhob@ufrrj.br/rastro_publico/src")
dbutils.widgets.dropdown("strategy", "natural", ["natural", "broadcast", "merge"])
dbutils.widgets.text("run_label", "iteracao-01")

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

initial_plan = spark.sql(build_explain_sql(strategy, f"plano-{run_label}"))
print("PLANO_INICIAL")
print("\n".join(row[0] for row in initial_plan.collect()))

result_rows = [list(row) for row in spark.sql(build_benchmark_sql(strategy, run_label)).collect()]
canonical_result = canonical_benchmark_result(result_rows)
output = {
    "strategy": strategy,
    "run_label": run_label,
    "result": canonical_result,
}
print(json.dumps(output, ensure_ascii=False))
dbutils.notebook.exit(json.dumps(output, ensure_ascii=False))
