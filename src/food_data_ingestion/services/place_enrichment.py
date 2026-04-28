"""Place Enrichment service：把 pending candidates 經 Text Search 映射到 Google Place 並寫 restaurant 骨架。

Stage A only：
 - 用 candidate.candidate_name + (address 可選) 做 text query
 - 拿 ESSENTIALS 欄位（最便宜）
 - 信心度規則：唯一結果，或 top1 地址 token 命中 candidate.address → matched
 - matched 時：用 search hit 的 essentials 寫 restaurants 骨架，回填 candidate.matched_place_id/restaurant_id
 - ambiguous / no_match / failed 都更新 candidate.match_status，不寫 restaurant

Stage B（補 rating/opening_hours 等 Pro 欄位）由另一個 job 處理，不在此實作。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
import re
from typing import Any, Callable, Protocol

from food_data_ingestion.models.parser_input import ParserInput
from food_data_ingestion.models.restaurant import (
    ParsedExternalRef,
    ParsedPlaceDetail,
    ParsedRestaurant,
)
from food_data_ingestion.parsers.google_places import (
    PlaceSearchHit,
    normalize_restaurant_name,
    parse_search_text,
)
from food_data_ingestion.storage.discovered_candidate_repository import PendingCandidate


_TOKEN_SPLIT_RE = re.compile(r"[\s,，、。\-\u3000]+")


class CandidateRepoProtocol(Protocol):
    def list_pending_for_match(self, *, limit: int) -> list[PendingCandidate]: ...

    def apply_match_result(
        self,
        *,
        candidate_id: int,
        match_status: str,
        matched_place_id: str | None,
        matched_restaurant_id: int | None,
        match_meta: dict[str, Any],
        attempt_at: datetime,
    ) -> None: ...


class ConnectorProtocol(Protocol):
    def search_text(self, text_query: str, **kwargs: Any) -> dict[str, Any]: ...


class RestaurantRepoProtocol(Protocol):
    def upsert(self, parsed: ParsedPlaceDetail) -> int: ...


class TransactionManagerProtocol(Protocol):
    def commit(self) -> None: ...

    def rollback(self) -> None: ...


@dataclass(frozen=True)
class CandidateMatchOutcome:
    candidate_id: int
    match_status: str
    matched_place_id: str | None = None
    matched_restaurant_id: int | None = None
    cache_hit: bool = False
    hit_count: int = 0
    reason: str = ""


@dataclass(frozen=True)
class EnrichmentReport:
    processed: int
    matched: int
    ambiguous: int
    no_match: int
    failed: int
    cache_hits: int
    outcomes: list[CandidateMatchOutcome]


def _tokenize(text: str | None) -> set[str]:
    if not text:
        return set()
    parts = _TOKEN_SPLIT_RE.split(text.strip().lower())
    return {p for p in parts if p}


def decide_match(candidate: PendingCandidate, hits: list[PlaceSearchHit]) -> tuple[str, int | None, str]:
    """回傳 (match_status, hit_index_to_use_or_None, reason)。

    規則（保守，避免錯配）：
      - 0 hit → no_match
      - 1 hit → matched（採用唯一結果）
      - >1 hit：
          * candidate 名稱在 top1.display_name 出現，且 candidate.address 提供時，
            address tokens 與 top1.formatted_address 至少 1 token 命中 → matched
          * 否則 ambiguous
    """
    if not hits:
        return "no_match", None, "search returned no places"

    if len(hits) == 1:
        return "matched", 0, "single hit"

    top = hits[0]
    cand_name_norm = normalize_restaurant_name(candidate.candidate_name).lower()
    top_name_norm = normalize_restaurant_name(top.display_name).lower()
    name_overlap = bool(cand_name_norm) and (cand_name_norm in top_name_norm or top_name_norm in cand_name_norm)
    if not name_overlap:
        return "ambiguous", None, f"top hit name mismatch ({len(hits)} candidates)"

    if candidate.address:
        cand_tokens = _tokenize(candidate.address)
        addr_tokens = _tokenize(top.formatted_address)
        token_overlap = bool(cand_tokens) and bool(addr_tokens) and bool(cand_tokens & addr_tokens)
        addr_norm = (top.formatted_address or "").lower()
        substring_overlap = any(tok and tok in addr_norm for tok in cand_tokens)
        if token_overlap or substring_overlap:
            return "matched", 0, "address overlap with top hit"
        return "ambiguous", None, f"top name matches but address mismatch ({len(hits)} candidates)"

    return "ambiguous", None, f"no address to disambiguate ({len(hits)} candidates)"


def _hit_to_parsed(hit: PlaceSearchHit, *, source_meta: dict[str, Any]) -> ParsedPlaceDetail:
    canonical = hit.display_name or hit.place_id
    restaurant = ParsedRestaurant(
        canonical_name=canonical,
        normalized_name=normalize_restaurant_name(canonical),
        address=hit.formatted_address,
        latitude=hit.latitude,
        longitude=hit.longitude,
        source_meta=source_meta,
    )
    external_url = (
        f"https://www.google.com/maps/place/?q=place_id:{hit.place_id}" if hit.place_id else None
    )
    refs = [
        ParsedExternalRef(
            platform="google_places",
            external_id=hit.place_id,
            external_url=external_url,
            ref_type="place_search",
            is_primary=True,
            metadata={"place_id": hit.place_id, "types": list(hit.types)},
        )
    ]
    return ParsedPlaceDetail(restaurant=restaurant, external_refs=refs, aliases=[])


def build_search_query(candidate: PendingCandidate) -> str:
    parts: list[str] = [candidate.candidate_name.strip()]
    if candidate.address:
        parts.append(candidate.address.strip())
    return " ".join(p for p in parts if p)


@dataclass
class PlaceEnrichmentService:
    candidate_repository: CandidateRepoProtocol
    connector: ConnectorProtocol
    restaurant_repository: RestaurantRepoProtocol
    transaction_manager: TransactionManagerProtocol | None = None
    now_provider: Callable[[], datetime] = lambda: datetime.now(UTC)

    def enrich_pending(self, *, limit: int, dry_run: bool = False) -> EnrichmentReport:
        pending = self.candidate_repository.list_pending_for_match(limit=limit)
        outcomes: list[CandidateMatchOutcome] = []
        counters = {"matched": 0, "ambiguous": 0, "no_match": 0, "failed": 0, "cache_hits": 0}

        for candidate in pending:
            outcome = self._process_one(candidate, dry_run=dry_run)
            outcomes.append(outcome)
            if outcome.cache_hit:
                counters["cache_hits"] += 1
            counters[outcome.match_status if outcome.match_status in counters else "failed"] += 1

        return EnrichmentReport(
            processed=len(pending),
            matched=counters["matched"],
            ambiguous=counters["ambiguous"],
            no_match=counters["no_match"],
            failed=counters["failed"],
            cache_hits=counters["cache_hits"],
            outcomes=outcomes,
        )

    def _process_one(
        self, candidate: PendingCandidate, *, dry_run: bool
    ) -> CandidateMatchOutcome:
        query = build_search_query(candidate)
        if not query:
            return self._record_outcome(
                candidate=candidate,
                status="failed",
                place_id=None,
                restaurant_id=None,
                cache_hit=False,
                hit_count=0,
                reason="empty query",
                dry_run=dry_run,
            )

        try:
            fetch_result = self.connector.search_text(query)
        except Exception as exc:  # noqa: BLE001 — 對外保持寬容，記錄即可
            return self._record_outcome(
                candidate=candidate,
                status="failed",
                place_id=None,
                restaurant_id=None,
                cache_hit=False,
                hit_count=0,
                reason=f"connector error: {exc}",
                dry_run=dry_run,
            )

        cache_hit = bool(fetch_result.get("source_meta", {}).get("cache_hit"))
        if fetch_result.get("is_error"):
            return self._record_outcome(
                candidate=candidate,
                status="failed",
                place_id=None,
                restaurant_id=None,
                cache_hit=cache_hit,
                hit_count=0,
                reason=f"search error: {fetch_result.get('error_message')}",
                dry_run=dry_run,
            )

        body = fetch_result.get("response_body")
        hits = parse_search_text(body if isinstance(body, dict) else None)
        status, idx, reason = decide_match(candidate, hits)

        if status != "matched" or idx is None:
            return self._record_outcome(
                candidate=candidate,
                status=status,
                place_id=None,
                restaurant_id=None,
                cache_hit=cache_hit,
                hit_count=len(hits),
                reason=reason,
                dry_run=dry_run,
            )

        chosen = hits[idx]
        if dry_run:
            return CandidateMatchOutcome(
                candidate_id=candidate.id,
                match_status=status,
                matched_place_id=chosen.place_id,
                matched_restaurant_id=None,
                cache_hit=cache_hit,
                hit_count=len(hits),
                reason=reason + " (dry-run)",
            )

        parsed = _hit_to_parsed(
            chosen,
            source_meta={
                "candidate_id": candidate.id,
                "source_platform": candidate.source_platform,
                "source_url": candidate.source_url,
                "search_query": query,
                "match_reason": reason,
            },
        )
        restaurant_id = self.restaurant_repository.upsert(parsed)
        return self._record_outcome(
            candidate=candidate,
            status="matched",
            place_id=chosen.place_id,
            restaurant_id=restaurant_id,
            cache_hit=cache_hit,
            hit_count=len(hits),
            reason=reason,
            dry_run=False,
        )

    def _record_outcome(
        self,
        *,
        candidate: PendingCandidate,
        status: str,
        place_id: str | None,
        restaurant_id: int | None,
        cache_hit: bool,
        hit_count: int,
        reason: str,
        dry_run: bool,
    ) -> CandidateMatchOutcome:
        if not dry_run:
            self.candidate_repository.apply_match_result(
                candidate_id=candidate.id,
                match_status=status,
                matched_place_id=place_id,
                matched_restaurant_id=restaurant_id,
                match_meta={
                    "reason": reason,
                    "hit_count": hit_count,
                    "cache_hit": cache_hit,
                },
                attempt_at=self.now_provider(),
            )
            if self.transaction_manager is not None:
                self.transaction_manager.commit()
        return CandidateMatchOutcome(
            candidate_id=candidate.id,
            match_status=status,
            matched_place_id=place_id,
            matched_restaurant_id=restaurant_id,
            cache_hit=cache_hit,
            hit_count=hit_count,
            reason=reason + (" (dry-run)" if dry_run else ""),
        )
