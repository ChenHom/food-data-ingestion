from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any

from food_data_ingestion.config import Settings
from food_data_ingestion.connectors.candylife import CandylifeLiveFetcher
from food_data_ingestion.db.connection import create_connection
from food_data_ingestion.db.psycopg_session import PsycopgSession
from food_data_ingestion.discovery.service import UnifiedDiscoveryIngestionService
from food_data_ingestion.models.raw_document import RawDocumentCreate
from food_data_ingestion.parser_profiles.candylife import CandylifeDiscoveryPolicy
from food_data_ingestion.parsers.candylife_feed import ArticleKind, parse_candylife_feed
from food_data_ingestion.services.article_discovery import CandylifeArticleDiscoveryService
from food_data_ingestion.storage import DiscoveredPlaceCandidateRepository, RawDocumentRepository, SourceTargetRepository


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


def create_db_backed_repositories(connection):
    session = PsycopgSession(connection)
    return RawDocumentRepository(session), DiscoveredPlaceCandidateRepository(session)


def run_candylife_discovery(
    *,
    fetcher,
    min_year: int = 2025,
    limit: int = 20,
    raw_repository=None,
    candidate_repository=None,
    source_target: dict[str, Any] | None = None,
) -> dict[str, Any]:
    crawl_policy = (source_target or {}).get('crawl_policy') or {}
    effective_min_year = int(crawl_policy.get('min_year', min_year))
    effective_limit = int(crawl_policy.get('limit', limit))
    feed_url = (source_target or {}).get('target_value')
    feed_xml = fetcher.fetch_feed(feed_url) if feed_url is not None else fetcher.fetch_feed()
    entries = parse_candylife_feed(feed_xml)
    policy = CandylifeDiscoveryPolicy(min_year=effective_min_year)
    eligible_entries = [entry for entry in entries if policy.should_process_entry(entry)][:effective_limit]

    raw_repository = raw_repository or InMemoryRawRepository()
    candidate_repository = candidate_repository or InMemoryCandidateRepository()
    unified_ingestion_service = UnifiedDiscoveryIngestionService(candidate_repository=candidate_repository)
    service = CandylifeArticleDiscoveryService(
        fetcher=fetcher,
        raw_repository=raw_repository,
        unified_ingestion_service=unified_ingestion_service,
        policy=policy,
        clock=lambda: datetime.now(timezone.utc),
    )

    article_summaries: list[dict[str, Any]] = []
    candidate_count = 0
    single_store_count = 0
    roundup_count = 0
    for entry in eligible_entries:
        result = service.discover_article(entry)
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
        'source_target_id': (source_target or {}).get('id'),
        'min_year': effective_min_year,
        'limit': effective_limit,
        'feed_entry_count': len(entries),
        'eligible_entry_count': len(eligible_entries),
        'processed_entry_count': len(article_summaries),
        'single_store_count': single_store_count,
        'roundup_count': roundup_count,
        'candidate_count': candidate_count,
        'articles': article_summaries,
    }


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
        source_target = None
        if args.write_db or args.source_target_id:
            connection = create_connection(Settings.from_env())
            if args.write_db:
                raw_repository, candidate_repository = create_db_backed_repositories(connection)
            if args.source_target_id:
                source_target = SourceTargetRepository(PsycopgSession(connection)).get_by_id(args.source_target_id)
                if source_target is None:
                    source_target = {'id': args.source_target_id, 'crawl_policy': {}}
        result = run_candylife_discovery(
            fetcher=fetcher,
            min_year=args.min_year,
            limit=args.limit,
            raw_repository=raw_repository,
            candidate_repository=candidate_repository,
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
