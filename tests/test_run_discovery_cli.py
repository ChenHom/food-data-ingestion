"""Unified discovery runner 的 end-to-end CLI smoke test。

取代了各來源的 `test_*_cli.py`。驗證：
- 單一 platform 跨出符合預期形狀的結果
- 多 platform（預設是所有已註冊者）跨出時以平行方式跨，
  每個 platform 都收到一個結果
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
    # 沒有 --write-db 就不會查 DB，所以 exclude id 實際上是 no-op，
    # 但 flag 仍需能 parse 並讓整個跨次成功。
    data = _run("--platform", "candylife", "--exclude-source-target-id", "999")
    assert data["summary"]["failed"] == 0
