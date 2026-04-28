"""Parsers for supertaste.tvbs.com.tw sitemaps.

Two stages:
  1. parse_supertaste_sitemap_index(xml) -> tuple[str, ...]
     Returns child sitemap URLs, optionally filtered to article sitemaps.
  2. parse_supertaste_sitemap(xml) -> tuple[SupertasteSitemapEntry, ...]
     Each <url> in an article sitemap becomes an entry with category + id parsed
     from the URL path (e.g. /pack/348872 -> category='pack', article_id='348872').
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
    """Parse the top-level sitemap index, returning child sitemap URLs.

    By default keeps only article_sitemap_*.xml entries since those are the
    ones that list article URLs we care about.
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
    """Parse an article sitemap into typed entries.

    Skips entries whose path does not match the /<category>/<id> pattern.
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
    # Strip scheme://host
    no_scheme = url.split("://", 1)[-1]
    slash = no_scheme.find("/")
    return no_scheme[slash:] if slash >= 0 else "/"
