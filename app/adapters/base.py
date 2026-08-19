"""
Every source is an adapter: fetch() gets raw data, parse() turns one raw
record into a validated JobListing. The pipeline never talks to a source
directly — it only knows this interface. That's what makes "add a new
source" or "fall back to source B" a config change instead of a rewrite.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable

import httpx

from app.models import JobListing


class SourceAdapter(ABC):
    name: str = "base"

    # Conservative, believable browser-shaped headers. This is a public,
    # scrape-friendly API/feed — no fingerprint evasion is needed here —
    # but sending a real UA + Accept headers instead of the default
    # "python-requests/x.y" string is baseline good citizenship for any
    # HTTP client talking to someone else's server.
    default_headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/xml, text/html, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    @abstractmethod
    async def fetch(self, client: httpx.AsyncClient) -> Any:
        """Hit the source, return raw response data (list of dicts, XML text, etc)."""
        ...

    @abstractmethod
    def to_records(self, raw: Any) -> Iterable[dict]:
        """Turn fetch()'s raw output into an iterable of plain dicts, one per job."""
        ...

    def parse(self, record: dict) -> JobListing:
        """
        Default parse: assumes record already roughly matches JobListing's
        shape. Adapters override this when the source's field names differ.
        """
        return JobListing(source=self.name, **record)
