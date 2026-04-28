"""針對 supertaste.tvbs.com.tw article JSON 的 parser。

輸入：`GET /api/article/{cat_en_name}/{id}` 回傳的 JSON body。
輸出：SupertasteArticleExtraction，內含從 `data.article_content.value` 內的
`info_card_app coupon` 區塊抽取出來的店家 candidate。

每個店家區塊都有固定的 HTML 屬性：
  <div class="info_card_app coupon" data-store_id="..." data-store_name="..."
       data-tag="..." data-keyword="...">
    ...
    <div class="store-address"><p>address</p>...
    <button class="call tel" data-tel="04-XXXXXXXX">
  </div>

Address 與 phone 都是 best-effort 抽取，可能缺少 — 抽取流程不會只因為選填欄位缺少就丟掉 candidate。
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
    """將每個 `info_card_app coupon` div 收集成一個扁平的 dict，包含屬性與 body 內的文字。

    我們不打算重現下嵌套的 HTML — 只需要 `store-address` 裡的段落文字以及
    `call tel` 的 data-tel 屬性來填選填欄位。其他資訊都來自 div 本身的屬性。
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
            # 從 void 的 <input>/<img> 取得 phone（它們可能帶有 data-tel），
            # 其他地方則從 `call tel` button 取 phone — 保持簡單。
            return
        # 位於一個 active 的 card 內部
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
            # store-address 裡的第一個 <p> 里面裝的就是地址
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


# 某些 article body 中間夸雜了多餘文字，會讓嚴格版的 parser 出錯；需要時回退使用
# regex 掃描（所幸這個結構化的 div 內容很規則）。
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
        # Regex fallback — 只能抽出屬性部分（拿不到 address/phone）。
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
