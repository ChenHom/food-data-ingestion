"""Unified discovery runner.

Replaces per-source `run_<x>_discovery.py` scripts. Picks adapters via the
registry, fans out across source_targets in parallel using ThreadPoolExecutor
(each worker gets its own DB connection), and collects per-target results.

CLI flags are intentionally minimal:
  --platform                  : restrict which platforms run (repeatable; default = all registered)
  --exclude-source-target-id  : ids to skip (repeatable)
  --max-workers               : cross-source parallelism (default 4)
  --use-stub-fetcher          : force stub fetchers (existing behaviour)
  --write-db                  : persist via DB-backed repos (no-op in pure stub mode)

Per-source knobs (min_year, limit, max_sitemaps, ...) belong in
`source_targets.crawl_policy` JSONB; the runner does not know them.
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from food_data_ingestion.config import Settings
from food_data_ingestion.db.connection import create_connection
from food_data_ingestion.db.psycopg_session import PsycopgSession
from food_data_ingestion.discovery.adapter import BuildContext, DiscoveryDeps
from food_data_ingestion.discovery.registry import DEFAULT_FACTORY, DiscoveryAdapterFactory
from food_data_ingestion.discovery.sources._shared import create_db_backed_repositories
from food_data_ingestion.storage import SourceTargetRepository


def _resolve_platforms(
    requested: list[str] | None,
    factory: DiscoveryAdapterFactory,
) -> list[str]:
    available = factory.platforms()
    if not requested:
        return available
    unknown = [p for p in requested if p not in available]
    if unknown:
        raise SystemExit(f"unknown platform(s): {unknown}; registered: {available}")
    return list(requested)


def _resolve_targets(
    *,
    platforms: list[str],
    exclude_ids: list[int],
    write_db: bool,
    settings: Settings | None,
) -> tuple[list[dict[str, Any]], Any | None]:
    """Return (tasks, lookup_connection).

    Each task is a dict with at least {"platform", "source_target"}. When
    write_db is False AND no DB connection is needed, source_target may be None
    (the adapter will fall back to its own defaults).
    """
    if not write_db:
        # Stub / dry-run mode: synthesise one task per requested platform.
        return [{"platform": p, "source_target": None} for p in platforms], None

    if settings is None:
        settings = Settings.from_env()
    connection = create_connection(settings)
    rows = SourceTargetRepository(PsycopgSession(connection)).list_enabled(
        platforms=platforms,
        exclude_ids=exclude_ids,
    )
    connection.close()

    if not rows:
        # No DB targets configured yet; still let each platform run with defaults.
        return [{"platform": p, "source_target": None} for p in platforms], None

    return [{"platform": row["platform"], "source_target": row} for row in rows], None


def _run_one(
    *,
    platform: str,
    source_target: dict[str, Any] | None,
    factory: DiscoveryAdapterFactory,
    build_ctx: BuildContext,
    write_db: bool,
    settings: Settings | None,
) -> dict[str, Any]:
    """Execute a single (platform, source_target) task in this worker thread."""
    adapter = factory.build(platform, build_ctx)
    connection = None
    try:
        if write_db:
            if settings is None:
                settings = Settings.from_env()
            connection = create_connection(settings)
            deps_dict = create_db_backed_repositories(connection)
            deps = DiscoveryDeps(
                raw_repository=deps_dict["raw_repository"],
                candidate_repository=deps_dict["candidate_repository"],
                crawl_job_repository=deps_dict["crawl_job_repository"],
                cache_repository=deps_dict["cache_repository"],
                transaction_manager=deps_dict["session"],
            )
        else:
            deps = DiscoveryDeps()

        result = adapter.run(source_target=source_target, deps=deps)

        if connection is not None:
            connection.commit()
        return {"status": "success", "platform": platform, "result": result}
    except Exception as exc:
        if connection is not None:
            try:
                connection.rollback()
            except Exception:  # pragma: no cover - defensive
                pass
        return {
            "status": "failed",
            "platform": platform,
            "source_target_id": (source_target or {}).get("id"),
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:  # pragma: no cover - defensive
                pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run discovery for all registered public sources in parallel.")
    parser.add_argument("--platform", action="append", default=None,
                        help="Restrict to this platform (repeatable). Default: all registered.")
    parser.add_argument("--exclude-source-target-id", action="append", type=int, default=None,
                        help="source_target id to skip (repeatable).")
    parser.add_argument("--max-workers", type=int, default=4,
                        help="Cross-source parallelism (default 4).")
    parser.add_argument("--use-stub-fetcher", action="store_true",
                        help="Force every adapter to use its stub fetcher.")
    parser.add_argument("--write-db", action="store_true",
                        help="Persist via DB-backed repositories.")
    args = parser.parse_args(argv)

    factory = DEFAULT_FACTORY
    platforms = _resolve_platforms(args.platform, factory)
    exclude_ids = list(args.exclude_source_target_id or [])

    settings: Settings | None = None
    tasks, _ = _resolve_targets(
        platforms=platforms,
        exclude_ids=exclude_ids,
        write_db=args.write_db,
        settings=settings,
    )

    build_ctx = BuildContext(use_stub_fetcher=args.use_stub_fetcher)

    results: list[dict[str, Any]] = []
    if not tasks:
        payload = {"results": [], "summary": {"success": 0, "failed": 0}}
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    max_workers = max(1, min(args.max_workers, len(tasks)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                _run_one,
                platform=task["platform"],
                source_target=task["source_target"],
                factory=factory,
                build_ctx=build_ctx,
                write_db=args.write_db,
                settings=settings,
            ): task
            for task in tasks
        }
        for future in as_completed(future_map):
            results.append(future.result())

    success_count = sum(1 for r in results if r["status"] == "success")
    failed_count = sum(1 for r in results if r["status"] == "failed")
    payload = {
        "results": results,
        "summary": {"success": success_count, "failed": failed_count, "total": len(results)},
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
