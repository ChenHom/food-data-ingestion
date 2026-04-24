from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from enum import Enum
import re
import xml.etree.ElementTree as ET


class ArticleKind(str, Enum):
    SINGLE_STORE = "single_store"
    ROUNDUP = "roundup"


@dataclass(frozen=True)
class CandylifeFeedEntry:
    title: str
    link: str
    published_at: str
    categories: tuple[str, ...]
    article_kind: ArticleKind

    @property
    def published_year(self) -> int:
        return parsedate_to_datetime(self.published_at).year


def classify_article_kind(*, title: str, categories: tuple[str, ...]) -> ArticleKind:
    lowered_title = title.lower()
    if "懶人包" in title or "特輯" in title or any("懶人包" in c or "特輯" in c for c in categories):
        return ArticleKind.ROUNDUP
    return ArticleKind.SINGLE_STORE


def parse_candylife_feed(xml_text: str) -> list[CandylifeFeedEntry]:
    root = ET.fromstring(xml_text.lstrip())
    channel = root.find("channel")
    if channel is None:
        return []

    entries: list[CandylifeFeedEntry] = []
    for item in channel.findall("item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        published_at = (item.findtext("pubDate") or "").strip()
        categories = tuple((node.text or "").strip() for node in item.findall("category") if (node.text or "").strip())
        if not title or not link or not published_at:
            continue
        entries.append(
            CandylifeFeedEntry(
                title=title,
                link=link,
                published_at=published_at,
                categories=categories,
                article_kind=classify_article_kind(title=title, categories=categories),
            )
        )
    return entries


def filter_recent_entries(entries: list[CandylifeFeedEntry], *, min_year: int = 2025) -> list[CandylifeFeedEntry]:
    return [entry for entry in entries if entry.published_year >= min_year]
