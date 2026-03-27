"""
Crawlers package — maps source names to crawler functions.
"""
from qdarchive.crawlers.harvard import crawl_harvard
from qdarchive.crawlers.harvard_oai import crawl_harvard_oai
from qdarchive.crawlers.columbia import crawl_columbia

CRAWLERS: dict = {
    "harvard":     crawl_harvard,
    "harvard-oai": crawl_harvard_oai,
    "columbia":    crawl_columbia,
}

ALL_SOURCES: list[str] = list(CRAWLERS.keys())

__all__ = ["CRAWLERS", "ALL_SOURCES", "crawl_harvard", "crawl_harvard_oai", "crawl_columbia"]
