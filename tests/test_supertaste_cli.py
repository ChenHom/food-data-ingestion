from __future__ import annotations

import json
import os
import subprocess
import sys


def test_supertaste_cli_supports_dry_run_with_stub_fetcher():
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "food_data_ingestion.jobs.run_supertaste_discovery",
            "--use-stub-fetcher",
            "--limit",
            "5",
        ],
        cwd="/home/hom/services/food-data-ingestion",
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(proc.stdout)
    assert data["limit"] == 5
    # Stub sitemap has 3 urls (pack/food/hot); default policy keeps pack+food only
    assert data["entry_count"] == 2
    assert data["processed_entry_count"] == 2
    assert data["roundup_count"] == 1
    assert data["single_count"] == 1
    # pack has 2 candidates, food has 1
    assert data["candidate_count"] == 3
    by_id = {a["article_id"]: a for a in data["articles"]}
    assert by_id["100001"]["article_kind"] == "roundup"
    assert by_id["100002"]["article_kind"] == "single_store"
