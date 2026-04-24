from __future__ import annotations

import json
import os
import subprocess
import sys


def test_candylife_cli_supports_source_target_id_with_stub_fetcher():
    env = os.environ.copy()
    env['PYTHONPATH'] = 'src'
    proc = subprocess.run(
        [
            sys.executable, '-m', 'food_data_ingestion.jobs.run_candylife_discovery',
            '--use-stub-fetcher',
            '--source-target-id', '42',
        ],
        cwd='/home/hom/services/food-data-ingestion',
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(proc.stdout)
    assert data['source_target_id'] == 42
