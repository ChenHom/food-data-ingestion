"""Parser for supertaste.tvbs.com.tw article JSON.

Input: the JSON body returned by `GET /api/article/{cat_en_name}/{id}`.
Output: SupertasteArticleExtraction with shop candidates extracted from the
`info_card_app coupon` blocks embedded in `data.article_content.value`.

Each shop block has stable HTML attributes:
  <div class="info_card_app coupon" data-store_id="..." data-store_name="..."
       data-tag="..." data-keyword="...">
    ...
    <div class="store-address"><p>address</p>...
    <button class="call tel" data-tel="04-XXXXXXXX">
  </div>

Address and phone are best-effort and may be missing — extraction never drops
a candidate just because of missing optional fields.
"""

from __future__ import annotations

from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from typing import Any
import re


@dataclass(frozen=True)
class SupertasteCandidate:
    external_id: str           # data-store_id
    name: str                  # data-store_name
    address: str | None
    phone: str | None
    tags: tuple[str, ...]
    keywords: tuple[str, ...]
    source_url: str


@dataclass(frozen=True)
class SupertasteArticleExtraction:
    source_url: str
    article_id: str
    category: str              # cat_en_name (e.g. 'pack', 'food')
    title: str
    description: str | None
    published_at: str | None   # use broadcast_date if present, else publish
    updated_at: str | None
    image_url: str | None
    tags: tuple[str, ...]
    candidates: tuple[SupertasteCandidate, ...]


# --- public entry point ----------------------------------------------------


def extract_supertaste_article(
    payload: dict[str, Any],
    *,
    source_url: str | None = None,
) -> SupertasteArticleExtraction:
    data = payload.get("data") or {}
    article_id = str(data.get("articles_id") or "")
    category = str(data.get("cat_en_name") or "")
    title = str(data.get("title") or "").strip()
    description = (data.get("description") or None)
    if isinstance(description, str):
        description = description.strip() or None
    image_url = (data.get("image") or None) or None
    broadcast = data.get("broadcast_date") or None
    publish = data.get("publish") or None
    updated = data.get("updated_time") or None
    tags = _split_csv(data.get("tag"))

    resolved_url = source_url or str(data.get("share_url") or "").strip() or ""
    article_content = data.get("article_content") or {}
    body_html = article_content.get("value") or ""

    candidates = _extract_candidates_from_html(body_html, source_url=resolved_url)

    return SupertasteArticleExtraction(
        source_url=resolved_url,
        article_id=article_id,
        category=category,
        title=title,
        description=description,
        published_at=str(broadcast or publish or "") or None,
        updated_at=str(updated) if updated else None,
        image_url=image_url,
        tags=tags,
        candidates=candidates,
    )


def _split_csv(value: Any) -> tuple[str, ...]:
    if not value or not isinstance(value, str):
        return ()
    parts = [p.strip() for p in value.split(",")]
    return tuple(p for p in parts if p)


# --- info_card_app extraction ---------------------------------------------


class _InfoCardCollector(HTMLParser):
    """Collect each `info_card_app coupon` div as a flat dict of attrs + body text spans.

    We don't try to fully reconstruct nested HTML — we only need the
    `store-address` paragraph text and `call tel` data-tel attribute for the
    optional fields. Everything else comes from the div's own attributes.
    """

    VOID_TAGS = {
        "area", "base", "br", "col", "embed", "hr", "img", "input",
        "link", "meta", "param", "source", "track", "wbr",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._current: dict[str, Any] | None = None
        self._depth = 0
        self._in_address = False
        self._address_depth = 0
        self._address_buf: list[str] = []
        self.cards: list[dict[str, Any]] = []

    def handle_starttag(self, tag: str, attrs):
        attr_map = dict(attrs)
        if tag == "div" and self._is_info_card(attr_map):
            self._current = {
                "store_id": (attr_map.get("data-store_id") or "").strip(),
                "store_name": (attr_map.get("data-store_name") or "").strip(),
                "tag": (attr_map.get("data-tag") or "").strip(),
                "keyword": (attr_map.get("data-keyword") or "").strip(),
                "address": None,
                "phone": None,
            }
            self._depth = 1
            return
        if self._current is None:
            return
        if tag in self.VOID_TAGS:
            # Capture phone from void <input>/<img> elements that carry data-tel,
            # and capture phone from `call tel` buttons elsewhere — keep simple.
            return
        # Inside an active card
        self._depth += 1
        css_class = (attr_map.get("class") or "")
        if tag == "div" and "store-address" in css_class.split() and self._current.get("address") is None:
            self._in_address = True
            self._address_depth = self._depth
            self._address_buf = []
        elif tag == "button" and "tel" in css_class.split():
            tel = (attr_map.get("data-tel") or "").strip()
            if tel and self._current.get("phone") is None:
                self._current["phone"] = tel
        elif tag == "p" and self._in_address and not self._current.get("address"):
            # the FIRST <p> inside store-address holds the address
            self._address_buf = []

    def handle_endtag(self, tag: str):
        if self._current is None:
            return
        if self._in_address and tag == "p" and self._current.get("address") is None:
            text = " ".join("".join(self._address_buf).split()).strip()
            if text:
                self._current["address"] = text
        if self._in_address and self._depth == self._address_depth and tag == "div":
            self._in_address = False
        self._depth -= 1
        if self._depth <= 0:
            self.cards.append(self._current)
            self._current = None
            self._depth = 0
            self._in_address = False
            self._address_buf = []

    def handle_data(self, data: str):
        if self._in_address and self._current is not None and self._current.get("address") is None:
            self._address_buf.append(data)

    @staticmethod
    def _is_info_card(attr_map: dict[str, str | None]) -> bool:
        css = (attr_map.get("class") or "").split()
        return "info_card_app" in css and "coupon" in css


# Some article bodies have stray text that breaks the strict parser; fall back
# to a regex scan if needed (the structured div is highly regular).
_INFO_CARD_RE = re.compile(
    r'<div class="info_card_app coupon"([^>]*)>',
    re.IGNORECASE,
)
_ATTR_RE = re.compile(r'data-(store_id|store_name|tag|keyword)="([^"]*)"')


def _extract_candidates_from_html(html: str, *, source_url: str) -> tuple[SupertasteCandidate, ...]:
    if not html:
        return ()
    parser = _InfoCardCollector()
    try:
        parser.feed(html)
        parser.close()
    except Exception:
        parser.cards = []

    if not parser.cards:
        # Regex fallback — covers attribute-only extraction (no address/phone).
        for match in _INFO_CARD_RE.finditer(html):
            attrs = dict(_ATTR_RE.findall(match.group(1)))
            if not attrs.get("store_id"):
                continue
            parser.cards.append(
                {
                    "store_id": attrs.get("store_id", "").strip(),
                    "store_name": unescape(attrs.get("store_name", "").strip()),
                    "tag": unescape(attrs.get("tag", "").strip()),
                    "keyword": unescape(attrs.get("keyword", "").strip()),
                    "address": None,
                    "phone": None,
                }
            )

    candidates: list[SupertasteCandidate] = []
    seen_ids: set[str] = set()
    for card in parser.cards:
        store_id = card.get("store_id") or ""
        if not store_id or store_id in seen_ids:
            continue
        seen_ids.add(store_id)
        candidates.append(
            SupertasteCandidate(
                external_id=store_id,
                name=unescape(card.get("store_name") or ""),
                address=card.get("address"),
                phone=card.get("phone"),
                tags=_split_csv(card.get("tag")),
                keywords=_split_csv(card.get("keyword")),
                source_url=source_url,
            )
        )
    return tuple(candidates)
