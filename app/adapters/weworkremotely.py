from __future__ import annotations

import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Iterable

import httpx

from app.adapters.base import SourceAdapter
from app.models import JobListing


class WeWorkRemotelyAdapter(SourceAdapter):
    """
    Fallback source. WeWorkRemotely publishes public per-category RSS feeds
    — again, meant for programmatic consumption (that's what RSS is for).
    Structurally different from RemoteOK on purpose: it proves the pipeline
    abstracts over "JSON API" and "XML feed" the same way, and it's the
    thing the orchestrator switches to if the primary source's circuit
    breaker trips.
    """

    name = "weworkremotely"
    url = "https://weworkremotely.com/categories/remote-programming-jobs.rss"

    async def fetch(self, client: httpx.AsyncClient) -> Any:
        resp = await client.get(self.url, headers=self.default_headers, timeout=15.0)
        resp.raise_for_status()
        return resp.text

    def to_records(self, raw: Any) -> Iterable[dict]:
        root = ET.fromstring(raw)
        for item in root.findall(".//item"):

            def text(tag: str) -> str:
                el = item.find(tag)
                return el.text.strip() if el is not None and el.text else ""

            title_raw = text("title")  # usually "Company: Job Title"
            company, _, title = title_raw.partition(":")
            yield {
                "title": title.strip() or title_raw,
                "company": company.strip() or "Unknown",
                "url": text("link"),
                "pub_date": text("pubDate"),
                "guid": text("guid"),
            }

    def parse(self, record: dict) -> JobListing:
        posted_at = None
        if record.get("pub_date"):
            try:
                posted_at = parsedate_to_datetime(record["pub_date"])
            except (ValueError, TypeError):
                posted_at = None

        source_id = record.get("guid") or hashlib.sha1(
            record.get("url", "").encode()
        ).hexdigest()

        return JobListing(
            source=self.name,
            source_id=source_id,
            title=record.get("title", ""),
            company=record.get("company", ""),
            location="Remote",
            url=record.get("url", ""),
            posted_at=posted_at,
            tags=[],
        )
