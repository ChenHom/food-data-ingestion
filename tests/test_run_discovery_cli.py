"""End-to-end CLI smoke tests for the unified discovery runner.

Replaces the per-source `test_*_cli.py` tests. Verifies:
- single platform run produces a result with the expected shape
- multi-platform run (default = all registered) runs them in parallel and
  collects one result per platform
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _run(*extra_args: str) -> dict:
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "food_data_ingestion.jobs.run_discovery",
            "--use-stub-fetcher",
            *extra_args,
        ],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(proc.stdout)


def test_run_discovery_cli_single_platform_candylife():
    data = _run("--platform", "candylife")
    assert data["summary"] == {"success": 1, "failed": 0, "total": 1}
    result = data["results"][0]
    assert result["status"] == "success"
    assert result["platform"] == "candylife"
    assert "processed_entry_count" in result["result"]


def test_run_discovery_cli_single_platform_supertaste():
    data = _run("--platform", "supertaste")
    assert data["summary"] == {"success": 1, "failed": 0, "total": 1}
    result = data["results"][0]
    assert result["status"] == "success"
    assert result["platform"] == "supertaste"
    assert result["result"]["candidate_count"] == 3


def test_run_discovery_cli_runs_all_registered_platforms_in_parallel():
    data = _run("--max-workers", "2")
    assert data["summary"]["success"] >= 2
    platforms = {r["platform"] for r in data["results"]}
    assert {"candylife", "supertaste"}.issubset(platforms)


def test_run_discovery_cli_accepts_exclude_source_target_id_flag():
    # Without --write-db there is no DB lookup so excluded ids are a no-op,
    # but the flag must still parse and the run must succeed.
    data = _run("--platform", "candylife", "--exclude-source-target-id", "999")
    assert data["summary"]["failed"] == 0
