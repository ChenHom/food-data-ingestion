from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from typing import Any

from food_data_ingestion.config import Settings
from food_data_ingestion.connectors.candylife import CandylifeConnector, CandylifeLiveFetcher
from food_data_ingestion.db.connection import create_connection
from food_data_ingestion.db.psycopg_session import PsycopgSession
from food_data_ingestion.discovery.service import UnifiedDiscoveryIngestionService
from food_data_ingestion.models.cache import ApiRequestCacheEntry
from food_data_ingestion.models.crawl_job import CrawlJobCreate
from food_data_ingestion.models.raw_document import RawDocumentCreate
from food_data_ingestion.parser_profiles.candylife import CandylifeDiscoveryPolicy
from food_data_ingestion.parsers.candylife_feed import ArticleKind
from food_data_ingestion.services.candylife_ingestion import (
    CandylifeArticleIngestion,
    CandylifeFeedIngestion,
)
from food_data_ingestion.services.ingestion_context import IngestionContext
from food_data_ingestion.storage import (
    ApiRequestCacheRepository,
    CrawlJobRepository,
    DiscoveredPlaceCandidateRepository,
    RawDocumentRepository,
    SourceTargetRepository,
)


class InMemoryRawRepository:
    def __init__(self) -> None:
        self.rows: list[RawDocumentCreate] = []

    def create(self, payload: RawDocumentCreate) -> int:
        self.rows.append(payload)
        return len(self.rows)


class InMemoryCandidateRepository:
    def __init__(self) -> None:
        self.saved: list[tuple[Any, list[Any]]] = []

    def save_discovered_candidates(self, *, article, candidates) -> list[int]:
        self.saved.append((article, candidates))
        return list(range(1, len(candidates) + 1))


class InMemoryCrawlJobRepository:
    def __init__(self) -> None:
        self.jobs: list[dict[str, Any]] = []

    def create(self, payload: CrawlJobCreate) -> int:
        self.jobs.append({"payload": payload, "status": "queued"})
        return len(self.jobs)

    def mark_running(self, job_id: int, *, started_at, worker_name: str | None = None) -> None:
        self.jobs[job_id - 1]["status"] = "running"

    def mark_success(self, job_id: int, *, finished_at, stats=None) -> None:
        self.jobs[job_id - 1].update(status="success", stats=stats, finished_at=finished_at)

    def mark_failed(self, job_id: int, *, finished_at, error_message, stats=None) -> None:
        self.jobs[job_id - 1].update(status="failed", error=error_message, stats=stats)

    def mark_skipped(self, job_id: int, *, finished_at, error_message, stats=None) -> None:
        self.jobs[job_id - 1].update(status="skipped", error=error_message, stats=stats)


class InMemoryCacheRepository:
    def __init__(self) -> None:
        self.upserts: list[ApiRequestCacheEntry] = []

    def get_valid(self, cache_key, *, as_of):
        return None

    def mark_hit(self, cache_key, *, accessed_at) -> None:
        return None

    def upsert(self, entry: ApiRequestCacheEntry) -> None:
        self.upserts.append(entry)


class StubCandylifeFetcher:
    def fetch_feed(self, url: str | None = None) -> str:
        return '''<?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0"><channel>
          <item><title>阿發現炒｜單店</title><link>https://candylife.tw/a/</link><pubDate>Thu, 23 Apr 2026 06:24:32 +0000</pubDate><category>台中美食</category></item>
          <item><title>台中乳酪蛋糕懶人包｜四間</title><link>https://candylife.tw/b/</link><pubDate>Mon, 20 Apr 2026 13:23:56 +0000</pubDate><category>懶人包特輯</category></item>
        </channel></rss>'''

    def fetch_html(self, url: str) -> str:
        if url.endswith('/a/'):
            return '<html><head><title>阿發現炒｜單店 - 糖糖\'s 享食生活</title></head><body><article><h1>阿發現炒｜單店</h1><p>《店家資訊》</p><p>店家：阿發現炒 電話：04-12345678 地址：台中市中區測試路1號 時間：10:00~18:00</p></article></body></html>'
        return '<html><head><title>台中乳酪蛋糕懶人包 - 糖糖\'s 享食生活</title></head><body><article><h1>台中乳酪蛋糕懶人包</h1></article></body></html>'


def create_db_backed_repositories(connection) -> dict[str, Any]:
    """Build a full set of DB-backed repos sharing one PsycopgSession.

    The session is also returned as `transaction_manager` so IngestionContext
    can commit between crawl_job creation and raw_document inserts (otherwise
    the FK from raw_documents.crawl_job_id is unsatisfiable).
    """
    session = PsycopgSession(connection)
    return {
        'session': session,
        'raw_repository': RawDocumentRepository(session),
        'candidate_repository': DiscoveredPlaceCandidateRepository(session),
        'crawl_job_repository': CrawlJobRepository(session),
        'cache_repository': ApiRequestCacheRepository(session),
    }


def run_candylife_discovery(
    *,
    fetcher,
    min_year: int = 2025,
    limit: int = 20,
    raw_repository=None,
    candidate_repository=None,
    crawl_job_repository=None,
    cache_repository=None,
    transaction_manager=None,
    source_target: dict[str, Any] | None = None,
) -> dict[str, Any]:
    crawl_policy = (source_target or {}).get('crawl_policy') or {}
    effective_min_year = int(crawl_policy.get('min_year', min_year))
    effective_limit = int(crawl_policy.get('limit', limit))
    feed_url = (source_target or {}).get('target_value')
    source_target_id = (source_target or {}).get('id')

    raw_repository = raw_repository or InMemoryRawRepository()
    candidate_repository = candidate_repository or InMemoryCandidateRepository()
    crawl_job_repository = crawl_job_repository or InMemoryCrawlJobRepository()
    cache_repository = cache_repository or InMemoryCacheRepository()

    connector = CandylifeConnector(
        cache_repository=cache_repository,
        fetcher=fetcher if isinstance(fetcher, CandylifeLiveFetcher) else _wrap_fetcher(fetcher),
    )
    ctx = IngestionContext(
        crawl_job_repository=crawl_job_repository,
        raw_repository=raw_repository,
        transaction_manager=transaction_manager,
    )
    unified_ingestion_service = UnifiedDiscoveryIngestionService(candidate_repository=candidate_repository)
    policy = CandylifeDiscoveryPolicy(min_year=effective_min_year)

    feed_flow = CandylifeFeedIngestion(ctx=ctx, connector=connector)
    article_flow = CandylifeArticleIngestion(
        ctx=ctx,
        connector=connector,
        unified_ingestion_service=unified_ingestion_service,
        policy=policy,
    )

    feed_result = feed_flow.ingest(feed_url, source_target_id=source_target_id)
    eligible_entries = [entry for entry in feed_result.entries if policy.should_process_entry(entry)][:effective_limit]

    article_summaries: list[dict[str, Any]] = []
    candidate_count = 0
    single_store_count = 0
    roundup_count = 0
    for entry in eligible_entries:
        result = article_flow.ingest(entry, source_target_id=source_target_id)
        if entry.article_kind is ArticleKind.SINGLE_STORE:
            single_store_count += 1
        else:
            roundup_count += 1
        candidate_count += result.candidate_count
        article_summaries.append(
            {
                'title': entry.title,
                'link': entry.link,
                'published_at': entry.published_at,
                'article_kind': entry.article_kind.value,
                'raw_document_id': result.raw_document_id,
                'candidate_count': result.candidate_count,
                'persisted_candidate_ids': result.persisted_candidate_ids,
            }
        )

    return {
        'source_target_id': source_target_id,
        'min_year': effective_min_year,
        'limit': effective_limit,
        'feed_entry_count': len(feed_result.entries),
        'eligible_entry_count': len(eligible_entries),
        'processed_entry_count': len(article_summaries),
        'single_store_count': single_store_count,
        'roundup_count': roundup_count,
        'candidate_count': candidate_count,
        'articles': article_summaries,
    }


class _FetcherAdapter:
    """Wrap a duck-typed fetcher (with fetch_feed/fetch_html) so CandylifeConnector can use it."""

    def __init__(self, inner) -> None:
        self._inner = inner

    def fetch_feed(self, url: str | None = None) -> str:
        try:
            return self._inner.fetch_feed(url)
        except TypeError:
            return self._inner.fetch_feed()

    def fetch_html(self, url: str) -> str:
        return self._inner.fetch_html(url)


def _wrap_fetcher(fetcher):
    return _FetcherAdapter(fetcher)


def main() -> None:
    parser = argparse.ArgumentParser(description='Run candylife discovery for 2025+ articles.')
    parser.add_argument('--min-year', type=int, default=2025)
    parser.add_argument('--limit', type=int, default=20)
    parser.add_argument('--use-stub-fetcher', action='store_true')
    parser.add_argument('--write-db', action='store_true')
    parser.add_argument('--source-target-id', type=int)
    args = parser.parse_args()

    fetcher = StubCandylifeFetcher() if args.use_stub_fetcher else CandylifeLiveFetcher()
    connection = None
    try:
        raw_repository = None
        candidate_repository = None
        crawl_job_repository = None
        cache_repository = None
        transaction_manager = None
        source_target = None
        if args.write_db or args.source_target_id:
            connection = create_connection(Settings.from_env())
            if args.write_db:
                deps = create_db_backed_repositories(connection)
                raw_repository = deps['raw_repository']
                candidate_repository = deps['candidate_repository']
                crawl_job_repository = deps['crawl_job_repository']
                cache_repository = deps['cache_repository']
                transaction_manager = deps['session']
                source_target_session = deps['session']
            else:
                source_target_session = PsycopgSession(connection)
            if args.source_target_id:
                source_target = SourceTargetRepository(source_target_session).get_by_id(args.source_target_id)
                if source_target is None:
                    source_target = {'id': args.source_target_id, 'crawl_policy': {}}
        result = run_candylife_discovery(
            fetcher=fetcher,
            min_year=args.min_year,
            limit=args.limit,
            raw_repository=raw_repository,
            candidate_repository=candidate_repository,
            crawl_job_repository=crawl_job_repository,
            cache_repository=cache_repository,
            transaction_manager=transaction_manager,
            source_target=source_target,
        )
        if connection is not None and args.write_db:
            connection.commit()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception:
        if connection is not None:
            connection.rollback()
        raise
    finally:
        if connection is not None:
            connection.close()


if __name__ == '__main__':
    main()
