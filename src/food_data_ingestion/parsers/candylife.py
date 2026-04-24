from __future__ import annotations

from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
import re


@dataclass(frozen=True)
class RestaurantCandidate:
    name: str
    address: str | None
    phone: str | None
    opening_hours: str | None
    source_url: str


@dataclass(frozen=True)
class CandylifeArticleExtraction:
    source_url: str
    title: str
    published_at: str | None
    categories: tuple[str, ...]
    restaurant_candidates: tuple[RestaurantCandidate, ...]


class _CandylifeHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._title_parts: list[str] = []
        self._h1_parts: list[str] = []
        self._current_anchor_href: str | None = None
        self._current_anchor_parts: list[str] = []
        self._paragraph_parts: list[str] = []
        self._in_title = False
        self._in_h1 = False
        self._in_paragraph = False
        self.categories: list[str] = []
        self.paragraphs: list[str] = []
        self.published_at: str | None = None

    def handle_starttag(self, tag: str, attrs):
        attr_map = dict(attrs)
        if tag == "title":
            self._in_title = True
        elif tag == "h1":
            self._in_h1 = True
        elif tag == "a":
            self._current_anchor_href = attr_map.get("href")
            self._current_anchor_parts = []
        elif tag == "p":
            self._in_paragraph = True
            self._paragraph_parts = []
        elif tag == "meta" and attr_map.get("property") == "article:published_time":
            self.published_at = attr_map.get("content")

    def handle_endtag(self, tag: str):
        if tag == "title":
            self._in_title = False
        elif tag == "h1":
            self._in_h1 = False
        elif tag == "a":
            text = "".join(self._current_anchor_parts).strip()
            if self._current_anchor_href and "/category/" in self._current_anchor_href and text:
                self.categories.append(text)
            self._current_anchor_href = None
            self._current_anchor_parts = []
        elif tag == "p":
            text = "".join(self._paragraph_parts).strip()
            if text:
                self.paragraphs.append(text)
            self._in_paragraph = False
            self._paragraph_parts = []

    def handle_data(self, data: str):
        if self._in_title:
            self._title_parts.append(data)
        if self._in_h1:
            self._h1_parts.append(data)
        if self._current_anchor_href is not None:
            self._current_anchor_parts.append(data)
        if self._in_paragraph:
            self._paragraph_parts.append(data)

    @property
    def title(self) -> str:
        return unescape("".join(self._title_parts).strip())

    @property
    def h1(self) -> str:
        return unescape("".join(self._h1_parts).strip())


def _clean_title(title: str, h1: str) -> str:
    if h1:
        return h1
    return title.split(" - ")[0].strip()


def _extract_store_info(paragraphs: list[str]) -> str | None:
    for idx, paragraph in enumerate(paragraphs):
        if "《店家資訊》" in paragraph:
            if idx + 1 < len(paragraphs):
                return paragraphs[idx + 1]
        if "店家：" in paragraph and "地址：" in paragraph:
            return paragraph
    return None


def _extract_candidate(info_line: str, source_url: str) -> RestaurantCandidate | None:
    name_match = re.search(r"店家：\s*([^|電話地址時間]+?)(?:\s*\||\s+電話：|\s+地址：|\s+時間：|$)", info_line)
    phone_match = re.search(r"電話：\s*([0-9\-()]+)", info_line)
    address_match = re.search(r"地址：\s*([^\s].*?)(?:\s+時間：|$)", info_line)
    hours_match = re.search(r"時間：\s*(.+)$", info_line)

    if not name_match:
        return None
    return RestaurantCandidate(
        name=name_match.group(1).strip(),
        address=address_match.group(1).strip() if address_match else None,
        phone=phone_match.group(1).strip() if phone_match else None,
        opening_hours=hours_match.group(1).strip() if hours_match else None,
        source_url=source_url,
    )


def extract_candylife_article(*, html: str, source_url: str) -> CandylifeArticleExtraction:
    parser = _CandylifeHTMLParser()
    parser.feed(html)

    info_line = _extract_store_info(parser.paragraphs)
    candidates: tuple[RestaurantCandidate, ...] = ()
    if info_line:
        candidate = _extract_candidate(info_line, source_url)
        if candidate is not None:
            candidates = (candidate,)

    seen = set()
    categories: list[str] = []
    for category in parser.categories:
        if category not in seen:
            seen.add(category)
            categories.append(category)

    return CandylifeArticleExtraction(
        source_url=source_url,
        title=_clean_title(parser.title, parser.h1),
        published_at=parser.published_at,
        categories=tuple(categories),
        restaurant_candidates=candidates,
    )
