from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

import httpx

from app.adapters.base import SourceAdapter
from app.models import JobListing


class RemoteOKAdapter(SourceAdapter):
    """
    Primary source. RemoteOK publishes a public JSON feed at /api explicitly
    intended for programmatic use (it's what powers their own RSS/embed
    widgets), so this is the "sandbox-equivalent" low-risk source the task
    scope calls for — no auth wall, no anti-bot layer to route around.
    """

    name = "remoteok"
    url = "https://remoteok.com/api"

    async def fetch(self, client: httpx.AsyncClient) -> Any:
        resp = await client.get(self.url, headers=self.default_headers, timeout=15.0)
        resp.raise_for_status()
        return resp.json()

    def to_records(self, raw: Any) -> Iterable[dict]:
        # First element of RemoteOK's response is a legend/metadata object,
        # not a job — a real example of "the source's shape has quirks you
        # have to defend against," which is exactly the kind of thing that
        # breaks naive scrapers.
        for item in raw:
            if not isinstance(item, dict) or "id" not in item:
                continue
            yield item

    def parse(self, record: dict) -> JobListing:
        posted_at = None
        epoch = record.get("epoch")
        if epoch:
            try:
                posted_at = datetime.utcfromtimestamp(int(epoch))
            except (ValueError, TypeError):
                posted_at = None

        return JobListing(
            source=self.name,
            source_id=str(record["id"]),
            title=record.get("position", ""),
            company=record.get("company", ""),
            location=record.get("location") or "Remote",
            url=record.get("url", ""),
            posted_at=posted_at,
            tags=record.get("tags", []) or [],
        )
