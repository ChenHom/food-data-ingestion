"""針對 supertaste.tvbs.com.tw sitemap 的 parser。

分為兩個階段：
  1. parse_supertaste_sitemap_index(xml) -> tuple[str, ...]
     回傳子 sitemap 的 URL，可以只保留 article sitemap。
  2. parse_supertaste_sitemap(xml) -> tuple[SupertasteSitemapEntry, ...]
     article sitemap 裡的每個 <url> 都會變成一個 entry，category 與 article id
     是從 URL 路徑解析出來（例如 /pack/348872 -> category='pack', article_id='348872'）。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
import xml.etree.ElementTree as ET


SITEMAP_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"

_ARTICLE_PATH_RE = re.compile(r"^/(?P<category>[a-z]+)/(?P<article_id>\d+)/?$")


@dataclass(frozen=True)
class SupertasteSitemapEntry:
    url: str
    category: str
    article_id: str
    lastmod: str | None = None

    @property
    def lastmod_dt(self) -> datetime | None:
        if not self.lastmod:
            return None
        try:
            return datetime.fromisoformat(self.lastmod)
        except ValueError:
            return None


def parse_supertaste_sitemap_index(xml_text: str, *, only_article: bool = True) -> tuple[str, ...]:
    """解析最上層的 sitemap index，回傳子 sitemap 的 URL。

    預設只保留 article_sitemap_*.xml，因為這些才是列出我們在意的 article URL 的 sitemap。
    """
    root = ET.fromstring(xml_text.lstrip())
    urls: list[str] = []
    for sitemap in root.findall(f"{SITEMAP_NS}sitemap"):
        loc = sitemap.findtext(f"{SITEMAP_NS}loc") or ""
        loc = loc.strip()
        if not loc:
            continue
        if only_article and "article_sitemap_" not in loc:
            continue
        urls.append(loc)
    return tuple(urls)


def parse_supertaste_sitemap(xml_text: str) -> tuple[SupertasteSitemapEntry, ...]:
    """將 article sitemap 解析為 typed entry。

    路徑不符合 /<category>/<id> 型態的 entry 會被跳過。
    """
    root = ET.fromstring(xml_text.lstrip())
    entries: list[SupertasteSitemapEntry] = []
    for url_node in root.findall(f"{SITEMAP_NS}url"):
        loc = (url_node.findtext(f"{SITEMAP_NS}loc") or "").strip()
        lastmod = (url_node.findtext(f"{SITEMAP_NS}lastmod") or "").strip() or None
        if not loc:
            continue
        path = _extract_path(loc)
        match = _ARTICLE_PATH_RE.match(path)
        if not match:
            continue
        entries.append(
            SupertasteSitemapEntry(
                url=loc,
                category=match.group("category"),
                article_id=match.group("article_id"),
                lastmod=lastmod,
            )
        )
    return tuple(entries)


def _extract_path(url: str) -> str:
    # 拿掉 scheme://host
    no_scheme = url.split("://", 1)[-1]
    slash = no_scheme.find("/")
    return no_scheme[slash:] if slash >= 0 else "/"
