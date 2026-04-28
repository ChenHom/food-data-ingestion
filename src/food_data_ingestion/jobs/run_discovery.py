"""統一的 discovery runner。

取代那些各來源一個的 `run_<x>_discovery.py` script。透過 registry 取得 adapter，
使用 ThreadPoolExecutor 平行出多個 source_target（每個 worker 有自己的 DB connection），
並收集每個 target 的執行結果。

CLI flag 刻意保持最少：
  --platform                  : 限定要跑哪些 platform（可重複；預設 = 所有已註冊的）
  --exclude-source-target-id  : 要跳過的 id（可重複）
  --max-workers               : 跨來源的平行度（預設 4）
  --use-stub-fetcher          : 強制使用 stub fetcher（保留舊行為）
  --write-db                  : 透過 DB-backed repo 轉為久久保存（純 stub 模式下無作用）

來源個別的設定（min_year、limit、max_sitemaps…）應該寫在
`source_targets.crawl_policy` JSONB；runner 不該認識這些。
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
        raise SystemExit(f"未註冊的 platform：{unknown}；已註冊：{available}")
    return list(requested)


def _resolve_targets(
    *,
    platforms: list[str],
    exclude_ids: list[int],
    write_db: bool,
    settings: Settings | None,
) -> tuple[list[dict[str, Any]], Any | None]:
    """回傳 (tasks, lookup_connection)。

    每個 task 是一個至少包含 {"platform", "source_target"} 的 dict。當
    write_db 為 False 且不需要 DB connection 時，source_target 可以是 None
    （adapter 會回退使用自己的預設值）。
    """
    if not write_db:
        # Stub / dry-run 模式：針對每個請求的 platform 各自合成一個 task。
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
        # 尚未在 DB 設定任何 target；仍然讓每個 platform 以預設值執行。
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
    """在這個 worker thread 上執行一個 (platform, source_target) task。"""
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
    parser = argparse.ArgumentParser(description="平行執行所有已註冊公開來源的 discovery。")
    parser.add_argument("--platform", action="append", default=None,
                        help="限定要跑的 platform（可重複）。預設：所有已註冊的。")
    parser.add_argument("--exclude-source-target-id", action="append", type=int, default=None,
                        help="要跳過的 source_target id（可重複）。")
    parser.add_argument("--max-workers", type=int, default=4,
                        help="跨來源的平行度（預設 4）。")
    parser.add_argument("--use-stub-fetcher", action="store_true",
                        help="強制每個 adapter 都使用其 stub fetcher。")
    parser.add_argument("--write-db", action="store_true",
                        help="透過 DB-backed repository 久久保存。")
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
