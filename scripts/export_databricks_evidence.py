"""Exporta evidências verificáveis do Databricks sem credenciais ou e-mail."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path


def _cli(profile: str, *args: str) -> dict:
    completed = subprocess.run(
        ["databricks", *args, "--profile", profile, "--output", "json"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return json.loads(completed.stdout)


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _sanitize(value: object) -> object:
    if isinstance(value, dict):
        return {key: _sanitize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    if isinstance(value, str):
        value = re.sub(
            r"/Workspace/Users/[^/]+/", "/Workspace/Users/<redacted>/", value
        )
        return re.sub(r"/Users/[^/]+/", "/Users/<redacted>/", value)
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="rastro-publico")
    parser.add_argument("--annual-job-id", default="399795155769573")
    parser.add_argument("--benchmark-job-id", default="79177278313280")
    parser.add_argument("--benchmark-task-run-id", default="757244372360020")
    parser.add_argument(
        "--dashboard-id", default="01f182507d3519de8cd5931bef2d613f"
    )
    parser.add_argument("--output", type=Path, default=Path("evidence/databricks"))
    args = parser.parse_args()

    for name, job_id in (
        ("annual-job", args.annual_job_id),
        ("benchmark-job", args.benchmark_job_id),
    ):
        job = _cli(args.profile, "jobs", "get", job_id)
        _write(
            args.output / f"{name}.json",
            _sanitize({"job_id": job["job_id"], "settings": job["settings"]}),
        )

    output = _cli(
        args.profile, "jobs", "get-run-output", args.benchmark_task_run_id
    )
    metadata = output["metadata"]
    evidence = {
        "metadata": {
            key: metadata.get(key)
            for key in (
                "run_id",
                "job_id",
                "run_name",
                "start_time",
                "end_time",
                "execution_duration",
                "state",
            )
        },
        "notebook_output": json.loads(output["notebook_output"]["result"]),
        "truncated": output["notebook_output"].get("truncated", False),
    }
    _write(args.output / "benchmark-run.json", evidence)

    dashboard = _cli(args.profile, "lakeview", "get", args.dashboard_id)
    dashboard_evidence = {
        key: dashboard.get(key)
        for key in (
            "dashboard_id",
            "display_name",
            "lifecycle_state",
            "create_time",
            "update_time",
            "warehouse_id",
            "path",
        )
    }
    dashboard_evidence["serialized_dashboard"] = json.loads(
        dashboard["serialized_dashboard"]
    )
    _write(args.output / "dashboard.json", _sanitize(dashboard_evidence))


if __name__ == "__main__":
    main()
