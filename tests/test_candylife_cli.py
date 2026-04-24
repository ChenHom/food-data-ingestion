from __future__ import annotations

import json
import os
import subprocess
import sys


def test_candylife_cli_supports_dry_run_with_stub_fetcher():
    env = os.environ.copy()
    env['PYTHONPATH'] = 'src'
    proc = subprocess.run(
        [
            sys.executable, '-m', 'food_data_ingestion.jobs.run_candylife_discovery',
            '--use-stub-fetcher',
            '--limit', '2',
            '--min-year', '2025',
        ],
        cwd='/home/hom/services/food-data-ingestion',
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(proc.stdout)
    assert data['min_year'] == 2025
    assert data['limit'] == 2
    assert 'processed_entry_count' in data
